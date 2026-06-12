"""
RAAS 직무 맞춤 온보딩 — 추천 질문 칩 생성 로직.

핵심 흐름:
  load_profiles()         → role_profiles.json 1회 로드(메모리 캐시)
  list_active_profiles()  → 가입/프로필 드롭다운용 (active=true만)
  build_suggestions(role, anomalies, *, N=6, peer_topics=None)
      → 칩 N개 dict 리스트. 3소스 결합: anomaly > profile > peer.

설계 원칙:
- 표준 라이브러리만 사용.
- 어떤 입력이 와도 graceful — role None/미매칭/profile 없음/anomalies 빈 list 모두 OK.
- peer_topics는 콜드스타트라 지금은 호출자가 항상 None을 넘김. 인터페이스만 미리 노출.
"""

import json
import os
from typing import Optional

_PROFILES_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "role_profiles.json")
_CACHE: dict = {}  # 메모리 캐시 (mtime 키)


def _load_raw() -> dict:
    """role_profiles.json 로드. mtime 변경 시 자동 재로드."""
    try:
        mtime = os.path.getmtime(_PROFILES_PATH)
    except OSError:
        return {}
    if _CACHE.get('_mtime') == mtime and 'profiles' in _CACHE:
        return _CACHE['profiles']
    try:
        with open(_PROFILES_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        # _meta 같은 메타 키는 분리
        profiles = {k: v for k, v in data.items()
                    if not k.startswith("_") and isinstance(v, dict)}
        _CACHE['_mtime'] = mtime
        _CACHE['profiles'] = profiles
        return profiles
    except Exception as e:
        print(f"[onboarding] role_profiles.json 로드 실패: {e}", flush=True)
        return _CACHE.get('profiles', {})


def load_profiles() -> dict:
    """전체 프로파일 dict 반환 ({role: profile_dict})."""
    return _load_raw()


def list_active_profiles() -> list:
    """active=true 프로파일만 list로 반환 — 드롭다운용.
    리턴 형태: [{role, team, jtbd}], 순서는 JSON 정의 순."""
    out = []
    for role, p in _load_raw().items():
        if not p.get("active"):
            continue
        out.append({
            "role": role,
            "team": p.get("team", ""),
            "jtbd": p.get("jtbd", ""),
        })
    return out


def get_profile(role: Optional[str]) -> Optional[dict]:
    """role → profile dict. 없으면 None."""
    if not role:
        return None
    return _load_raw().get(role)


# ── 이상신호 매칭 ────────────────────────────────────────────
def _alert_matches_role(alert: dict, alert_match: dict) -> bool:
    """alert가 role의 alert_match 룰에 부합하는지."""
    cats   = alert_match.get("categories") or []
    fields = alert_match.get("fields") or []
    if cats and alert.get("category") not in cats:
        return False
    if not fields:
        return True
    if "*" in fields:
        return True
    return alert.get("field") in fields


_LEVEL_PRIORITY = {"red": 0, "yellow": 1, "green": 2}


def _sort_alerts(alerts: list) -> list:
    """red > yellow > green 순, z-score 절대값 큰 순."""
    def _key(a):
        lv = _LEVEL_PRIORITY.get(a.get("level"), 9)
        z  = abs(a.get("z") or 0)
        return (lv, -z)
    return sorted(alerts, key=_key)


# ── 이상신호 → 질문 텍스트 변환 ─────────────────────────────
_CHANNEL_NAMES = {
    "T00": "전체", "F00": "파워FM", "L00": "러브FM", "G00": "고릴라M", "P00": "픽채널"
}

# field → 질문에 쓸 한글 토픽
_FIELD_TOPIC = {
    "dau_chg": "DAU",     "dau": "DAU",
    "deep_rate_diff": "깊은청취율", "deep_rate": "깊은청취율",
    "real_rate_diff": "실청취율",   "real_rate": "실청취율",
    "engage_rate_diff": "참여율",   "engage_rate": "참여율",
    "churn_rate": "이탈율",         "churn_rate_diff": "이탈율",
    "react_rate": "복귀율",         "react_rate_diff": "복귀율",
    "new_chg": "신규 사용자",        "new_share": "신규 비중",
    "habit_rate": "습관형성률",
    "d1_ret": "D1 유지율",          "d1_ret_diff": "D1 유지율",
    "d7_ret": "D7 유지율",          "d7_ret_diff": "D7 유지율",
    "w1_ret": "W1 유지율",          "w1_ret_diff": "W1 유지율",
    "m1_ret": "M1 유지율",          "m1_ret_diff": "M1 유지율",
    "mau_chg": "MAU",               "mau": "MAU",
    "wau_chg": "WAU",
    "dau_1min": "1분이상 청취자",
    "dau_10min": "10분이상 청취자",
}


def _resolve_entity_name(code: str, category: str) -> str:
    """code → 한글 entity 이름. 프로그램이면 온톨로지에서 조회, 채널이면 매핑, 플랫폼이면 빈문자."""
    if category == "platform" or not code:
        return ""
    if code in _CHANNEL_NAMES:
        return _CHANNEL_NAMES[code]
    # 프로그램 — 온톨로지에서 정식명 조회
    try:
        from raas_onto import get_adapter
        meta = get_adapter().get_program_meta(code)
        if meta and meta.get("label"):
            return meta["label"]
    except Exception:
        pass
    return code  # fallback: 코드 그대로


def _alert_to_chip(alert: dict) -> dict:
    """단일 alert → 칩 dict.
    question은 RAAS 질의로 그대로 던질 수 있는 자연어 문장."""
    code     = alert.get("code") or ""
    category = alert.get("category") or "platform"
    field    = alert.get("field") or ""
    label    = alert.get("label") or ""
    topic    = _FIELD_TOPIC.get(field, label)
    entity   = _resolve_entity_name(code, category)

    # 질문 텍스트 — entity가 있으면 "{entity} {topic} 왜 변했어? 원인 분석해줘"
    if category == "platform":
        question = f"전체 {topic} 왜 변했어? 원인 분석해줘"
    else:
        question = f"{entity} {topic} 왜 그런지 볼까요?"

    return {
        "question": question,
        "source":   "anomaly",
        "level":    alert.get("level"),
        "reason":   alert.get("msg", "").strip(),
    }


# ── 메인 진입점 ──────────────────────────────────────────────
_DEFAULT_FALLBACK_ROLE = "총괄관리"


def build_suggestions(role: Optional[str], anomalies: Optional[list] = None,
                      N: int = 6, user_id=None,
                      peer_topics: Optional[list] = None) -> list:
    """직무 기반 추천 칩 N개 반환.

    Args:
        role:        users.role 저장값. None/미매칭이면 '총괄관리' 프로필로 폴백.
        anomalies:   s7_anomalies['alerts'] 리스트. 없거나 빈 list여도 동작.
        N:           반환할 칩 수.
        user_id:     로그인 사용자 ID. 있으면 recommend_for_user로 개인화 칩 합류.
        peer_topics: legacy 호환용 — 호출자가 None 전달이면 무시.

    배치 우선순위: anomaly → 개인화 추천(gap > peer > similar) → profile 템플릿 → legacy peer.

    Returns:
        [{question, source: 'anomaly'|'gap'|'gap-metric'|'peer'|'similar'|'profile',
          level?, reason?}]
    """
    profile = get_profile(role) or get_profile(_DEFAULT_FALLBACK_ROLE) or {}
    chips: list = []
    seen_questions: set = set()

    # ── 1) Anomaly 칩 (상위 2개까지) ─────────────────────────
    if anomalies and profile.get("alert_match"):
        matched = [a for a in anomalies
                   if a.get("level") in ("red", "yellow")
                   and _alert_matches_role(a, profile["alert_match"])]
        for a in _sort_alerts(matched)[:2]:
            chip = _alert_to_chip(a)
            if chip["question"] in seen_questions:
                continue
            chips.append(chip)
            seen_questions.add(chip["question"])

    # ── 2) 개인화 추천 (gap → peer → similar) ──────────────
    if user_id is not None:
        try:
            from raas_recommend import recommend_for_user  # lazy import — 순환 회피
            # 남은 자리 절반 정도 할당 (anomaly 자리 보장 + profile 폴백 자리)
            rec_limit = max(0, N - len(chips) - 1)
            for rec in recommend_for_user(user_id, role, limit=rec_limit):
                if len(chips) >= N:
                    break
                if rec.get("question") in seen_questions:
                    continue
                chips.append(rec)
                seen_questions.add(rec["question"])
        except Exception:
            pass  # 추천 실패해도 anomaly+profile 흐름 유지

    # ── 3) Profile 템플릿으로 채우기 (안정적 폴백) ─────────
    for q in profile.get("templates") or []:
        if len(chips) >= N:
            break
        if q in seen_questions:
            continue
        chips.append({"question": q, "source": "profile"})
        seen_questions.add(q)

    # ── 4) legacy peer_topics (호환용 — 보통 None) ──────────
    if peer_topics:
        for q in peer_topics:
            if len(chips) >= N:
                break
            if q in seen_questions:
                continue
            chips.append({"question": q, "source": "peer"})
            seen_questions.add(q)

    return chips[:N]
