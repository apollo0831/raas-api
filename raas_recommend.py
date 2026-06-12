"""
RAAS 추천 엔진 (Phase 4) — 안 본 것 중 봐야 할 것.

설계:
  recommend_for_user(user_id, role, limit) -> list[dict]
    각 항목: {question, source: 'gap'|'gap-metric'|'peer'|'similar',
              reason, score}

소스(가중 결합 + 임계점):
  - 'gap'        — role_profiles.json templates 중 본인이 안 던진 것    (score 3.0)
  - 'gap-metric' — role_profiles.json metrics 중 본인이 안 본 지표        (score 2.5)
  - 'peer'       — 같은 role 동료 인기 질문 (같은 role 사용자 ≥2명)        (score 2.0)
  - 'similar'    — 관심 분포(코사인) 유사 사용자가 본 주제 (같은 role ≥3명) (score 1.5)

원칙:
  - 표준 라이브러리만 (numpy 등 금지). 코사인 유사도 직접 구현.
  - 어떤 입력/예외에도 절대 raise 하지 않음 — 빈 list 반환으로 graceful.
  - fact null 행은 모든 SQL에서 자동 제외.
  - 본인 행은 peer/similar에서 항상 exclude.
"""

import math
from typing import Optional, Iterable

from raas_history_db import get_conn
from raas_onboarding import get_profile
from raas_querymap import get_popular_by_role


# ── 지표 라벨 → 내부 metric 코드 매핑 ────────────────────────
# classify_intent가 채우는 metric 값 (raas_query_engine INTENT_SYSTEM 참고).
# 매핑 불가한 라벨(예: '게스트/선곡 효과', '전 직무 요약')은 skip — 차라리 추천 안 함.
_LABEL_TO_METRIC: dict = {
    "DAU": {"dau"},
    "WAU": {"wau"},
    "MAU": {"mau"},
    "MAU(도달)": {"mau"},
    "DAU/MAU(도달)": {"dau", "mau"},
    "신규": {"new"},
    "신규/복귀": {"new", "react"},
    "복귀율": {"react_rate"},
    "이탈률": {"churn"},
    "실청취율": {"real"},
    "깊은청취율": {"deep"},
    "참여율": {"engage"},
    "습관형성률": {"habit"},
    "유지율": {"d1", "d7", "w1", "m1"},
    "유지율(코호트)": {"d1", "d7", "w1", "m1"},
    "리텐션": {"d1", "d7", "w1", "m1"},
    "장기 리텐션": {"w1", "m1"},
    "WoW": {"dau"},  # 진단용
    "시간대 DAU": {"dau"},
    "시간대별 청취자": {"dau", "dau_1min"},
    "프로그램별 청취자 규모": {"dau"},
}


def _label_to_metric_codes(label: str) -> set:
    """라벨(예: '깊은청취율') → 내부 metric 코드 set. 매핑 없으면 빈 set."""
    if not label:
        return set()
    # 정확 일치 우선
    if label in _LABEL_TO_METRIC:
        return _LABEL_TO_METRIC[label]
    # 키 부분포함 (예: '시간대 DAU' → 'DAU' 매핑은 이미 있으나, 임의 변종 보호)
    for k, v in _LABEL_TO_METRIC.items():
        if k in label:
            return v
    return set()


def _normalize_question(q: str) -> str:
    """질문 텍스트 정규화 — 공백/구두점 제거 + 소문자."""
    if not q:
        return ""
    return "".join(ch.lower() for ch in q if not ch.isspace() and ch not in "?!.,;:·~()[]{}\"'`")


# ── 사용자 이력 로드 ────────────────────────────────────────
def _get_user_history(user_id: str) -> dict:
    """user_id의 질의 fact set. 한 번에 조회해 dict로 반환."""
    out = {"questions": set(), "metrics": set(), "topic_keys": set(), "scopes": set()}
    if user_id is None:
        return out
    try:
        with get_conn() as conn:
            rows = conn.execute(
                "SELECT question, metric, scope, topic_key, metrics_json "
                "FROM query_history "
                "WHERE user_id = ? AND intent IS NOT NULL",
                (str(user_id),)
            ).fetchall()
        for r in rows:
            if r["question"]:
                out["questions"].add(_normalize_question(r["question"]))
            if r["metric"]:
                out["metrics"].add(r["metric"])
            if r["scope"]:
                out["scopes"].add(r["scope"])
            if r["topic_key"]:
                out["topic_keys"].add(r["topic_key"])
            # metrics_json도 metric 집합에 합치기 (trend·dual_trend가 배열로 채움)
            if r["metrics_json"]:
                try:
                    import json
                    arr = json.loads(r["metrics_json"])
                    for m in arr or []:
                        if m: out["metrics"].add(m)
                except Exception:
                    pass
    except Exception:
        pass
    return out


def _question_covered(template: str, user_questions: set) -> bool:
    """본인이 이미 그 질문을 던졌는지 — 정규화 부분일치 양방향."""
    if not template:
        return True
    norm = _normalize_question(template)
    if not norm:
        return True
    if norm in user_questions:
        return True
    # 양방향 부분 포함 (짧은 쪽이 긴 쪽에 들어가면 covered로 간주)
    for q in user_questions:
        if not q:
            continue
        short, long = (norm, q) if len(norm) <= len(q) else (q, norm)
        if len(short) >= 6 and short in long:
            return True
    return False


# ── 코사인 유사도 ────────────────────────────────────────────
def _cosine(v1: dict, v2: dict) -> float:
    """두 sparse 벡터(dict[str,number])의 코사인 유사도. 표준 라이브러리만."""
    if not v1 or not v2:
        return 0.0
    common = set(v1) & set(v2)
    if not common:
        return 0.0
    dot = sum(v1[k] * v2[k] for k in common)
    n1  = math.sqrt(sum(v * v for v in v1.values()))
    n2  = math.sqrt(sum(v * v for v in v2.values()))
    if n1 == 0 or n2 == 0:
        return 0.0
    return dot / (n1 * n2)


def _user_vectors_by_role(role: str, exclude_user_id: Optional[str] = None) -> dict:
    """같은 role 내 사용자들의 (metric+scope) 빈도 벡터 dict.
    벡터 키는 'm:dau', 's:T00' 같은 prefix로 metric/scope 구분."""
    out: dict = {}
    if not role:
        return out
    try:
        with get_conn() as conn:
            rows = conn.execute(
                "SELECT user_id, metric, scope FROM query_history "
                "WHERE user_role = ? AND intent IS NOT NULL",
                (role,)
            ).fetchall()
        for r in rows:
            uid = r["user_id"]
            if not uid or (exclude_user_id and str(uid) == str(exclude_user_id)):
                continue
            vec = out.setdefault(uid, {})
            if r["metric"]:
                k = f"m:{r['metric']}"
                vec[k] = vec.get(k, 0) + 1
            if r["scope"]:
                k = f"s:{r['scope']}"
                vec[k] = vec.get(k, 0) + 1
    except Exception:
        pass
    return out


def _build_my_vector(history: dict) -> dict:
    """내 이력에서 코사인 비교용 벡터 빌드. 단순 1-count (있음/없음)."""
    vec: dict = {}
    for m in history.get("metrics") or set():
        vec[f"m:{m}"] = 1
    for s in history.get("scopes") or set():
        vec[f"s:{s}"] = 1
    return vec


def _similar_users_topics(role: str, me_user_id: str, my_vec: dict,
                          top_k: int = 2, min_sim: float = 0.2) -> list:
    """관심 분포가 비슷한 동료의 topic_key/question 추출.
    임계점: 같은 role 내 본인 외 사용자 ≥3명."""
    others = _user_vectors_by_role(role, exclude_user_id=me_user_id)
    if len(others) < 3:
        return []
    if not my_vec:
        return []
    ranked = sorted(
        ((uid, _cosine(my_vec, v)) for uid, v in others.items()),
        key=lambda kv: -kv[1]
    )
    ranked = [(uid, s) for uid, s in ranked if s >= min_sim][:top_k]
    if not ranked:
        return []
    sim_user_ids = [uid for uid, _ in ranked]
    placeholders = ",".join(["?"] * len(sim_user_ids))
    try:
        with get_conn() as conn:
            rows = conn.execute(
                f"SELECT question, COUNT(*) AS c FROM query_history "
                f"WHERE user_id IN ({placeholders}) AND intent IS NOT NULL "
                f"GROUP BY question ORDER BY c DESC LIMIT 10",
                sim_user_ids
            ).fetchall()
        return [r["question"] for r in rows]
    except Exception:
        return []


def _same_role_user_count(role: str, exclude_user_id: Optional[str] = None) -> int:
    """같은 role을 가진 distinct user 수 (peer 임계점 판정)."""
    if not role:
        return 0
    try:
        with get_conn() as conn:
            sql = ("SELECT COUNT(DISTINCT user_id) FROM query_history "
                   "WHERE user_role = ? AND intent IS NOT NULL")
            params = [role]
            if exclude_user_id:
                sql += " AND user_id <> ?"
                params.append(str(exclude_user_id))
            return conn.execute(sql, params).fetchone()[0] or 0
    except Exception:
        return 0


# ── 메인 진입점 ──────────────────────────────────────────────
def recommend_for_user(user_id, role: Optional[str], limit: int = 5) -> list:
    """안 본 것 중 봐야 할 것 추천. 어떤 입력에서도 list 반환."""
    if limit <= 0:
        return []
    profile = get_profile(role) or {}
    history = _get_user_history(user_id)
    user_questions = history["questions"]
    user_metrics   = history["metrics"]

    candidates: list = []

    # ── 1) gap-template ─────────────────────────────────────
    for tmpl in profile.get("templates") or []:
        if _question_covered(tmpl, user_questions):
            continue
        candidates.append({
            "question": tmpl,
            "source":   "gap",
            "score":    3.0,
            "reason":   f"{role or '직무'} 직무에서 자주 살펴보는 주제예요",
        })

    # ── 2) gap-metric ───────────────────────────────────────
    for label in profile.get("metrics") or []:
        codes = _label_to_metric_codes(label)
        if not codes:
            continue
        if codes & user_metrics:
            continue  # 이미 본 지표 (어느 하나라도)
        q = f"전체 {label} 어때?"
        if _question_covered(q, user_questions):
            continue
        candidates.append({
            "question": q,
            "source":   "gap-metric",
            "score":    2.5,
            "reason":   f"{role or '직무'} 직무에서 챙기기 좋은 지표예요",
        })

    # ── 3) peer (같은 role 동료 ≥1명 있을 때만 호출 — 호출자 보호) ──
    if role and _same_role_user_count(role, exclude_user_id=str(user_id)) >= 1:
        try:
            peer_qs = get_popular_by_role(role, limit=5, days=30,
                                          exclude_user_id=str(user_id))
            for q in peer_qs:
                if _question_covered(q, user_questions):
                    continue
                candidates.append({
                    "question": q,
                    "source":   "peer",
                    "score":    2.0,
                    "reason":   "같은 직무 동료들이 자주 살펴보는 주제예요",
                })
        except Exception:
            pass

    # ── 4) similar (관심 분포 유사 사용자, 같은 role ≥3명일 때만) ──
    try:
        my_vec = _build_my_vector(history)
        sim_qs = _similar_users_topics(role, str(user_id), my_vec)
        for q in sim_qs:
            if _question_covered(q, user_questions):
                continue
            candidates.append({
                "question": q,
                "source":   "similar",
                "score":    1.5,
                "reason":   "관심 분포가 비슷한 동료가 본 주제예요",
            })
    except Exception:
        pass

    # ── 중복 제거 (정규화 기준) + score 내림차순 ────────────
    seen: set = set()
    ranked = sorted(candidates, key=lambda c: -c.get("score", 0))
    out: list = []
    for c in ranked:
        key = _normalize_question(c["question"])
        if key in seen:
            continue
        seen.add(key)
        out.append(c)
        if len(out) >= limit:
            break
    return out
