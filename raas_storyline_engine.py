"""raas_storyline_engine.py — 직무별 대화형 스토리라인 백엔드 엔진.

`data/storylines/{role}.json` 5슬롯 정의를 읽고 실제 KPI 데이터로 답변 렌더링.

엔드포인트 4종(raas_server.py)에서 호출:
    GET  /api/storyline/role-detect    → role_detect(user)
    GET  /api/storyline/entry          → StorylineEngine.entry()
    POST /api/storyline/advance        → StorylineEngine.advance(...)
    POST /api/storyline/export         → export_stub(...)  (Phase 3 ⑥에서 본격 구현)

설계 원칙:
    - JSON이 단일 출처. 코드는 데이터 채우기 + 렌더링만.
    - 슬롯별 데이터 계산은 _compute_slot_data() 디스패치 — 데이터 적재되면 함수만 채우면 됨.
    - 데이터 부족 시 자동 fallback_answer_template 사용.
    - 시뮬레이터(raas_storyline_simulator.py)의 render() 로직과 동일.
"""
from __future__ import annotations

import csv
import hashlib
import json
import re
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).parent
STORYLINES_DIR = ROOT / "data" / "storylines"
ONTO_DIR = ROOT / "raas_onto"   # 형식 TTL 온톨로지 (raas_ontology_*.ttl)
KPI_CSV = ROOT / "data" / "raas_kpi_latest.csv"

# ─── 직무 매핑 ──────────────────────────────────────────────────────────
# (1) role 식별자 → JSON 파일명
ROLE_TO_FILE = {
    "cp": "cp_v2.json",      # v1(cp.json) 폐기 — cp는 cp_v2로 단일화
    "cp_v2": "cp_v2.json",
    "pd_program": "pd_program.json",
    "제작pd": "pd_program.json",
    "pd_schedule": "pd_schedule.json",
    "편성pd": "pd_schedule.json",
}

# (2) 한국어 role(raas_auth.ALLOWED_ROLES) → 영문 role ID
KOREAN_ROLE_TO_ID = {
    "cp": "cp",
    "CP": "cp",
    "제작": "pd_program",       # raas_auth ALLOWED_ROLES와 일치
    "제작pd": "pd_program",     # 호환
    "제작PD": "pd_program",     # 호환
    "편성": "pd_schedule",       # raas_auth ALLOWED_ROLES와 일치
    "편성pd": "pd_schedule",     # 호환
    "편성PD": "pd_schedule",     # 호환
}

# (3) CP 채널 매핑 — 사용자 프로파일에 channel 필드 없으면 my_programs 첫 글자로 추론
PROGRAM_PREFIX_TO_CHANNEL = {
    "F": "파워FM",
    "L": "러브FM",
    "M": "러브FM",  # M05~M11도 러브FM 소속
    "G": "고릴라M",
    "P": "픽채널",
    "T": "전체",
}

CHANNEL_TO_PROGRAM_PREFIXES = {
    "파워FM": ["F"],          # F01~F13 (F00 제외)
    "러브FM": ["L", "M"],     # L01~L15, M05~M11 (L00 제외)
    "고릴라M": ["G"],
    "픽채널": ["P"],
}

# ─── 템플릿 렌더링 ──────────────────────────────────────────────────────
def render(template: str, data: dict) -> str:
    """{변수} 또는 {변수:포맷} 치환. 누락 변수는 [{변수명}?] 표시.

    raas_storyline_simulator.py와 동일 로직.
    """
    if not template:
        return ""

    def repl(m: re.Match) -> str:
        var_full = m.group(1)
        var_name = var_full.split(":")[0] if ":" in var_full else var_full
        if var_name not in data:
            return f"[{var_name}?]"
        try:
            return ("{" + var_full + "}").format_map(data)
        except (ValueError, KeyError, TypeError):
            return f"[{var_name}?]"

    return re.sub(r"\{([^{}]+)\}", repl, template)


# ─── JSON 설정 로더 (캐시) ──────────────────────────────────────────────
_CONFIG_CACHE: dict[str, dict] = {}


def load_config(role: str) -> dict:
    """role ID → 스토리라인 JSON dict. 캐시."""
    key = (role or "").lower().strip()
    if key in _CONFIG_CACHE:
        return _CONFIG_CACHE[key]

    fname = ROLE_TO_FILE.get(key)
    if not fname:
        raise ValueError(f"Unknown role: '{role}'. 사용 가능: {list_available_roles()}")

    cfg_path = STORYLINES_DIR / fname
    if not cfg_path.exists():
        raise FileNotFoundError(str(cfg_path))

    cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
    _CONFIG_CACHE[key] = cfg
    return cfg


def list_available_roles() -> list[str]:
    """사용 가능한 직무 ID 목록 (JSON 파일이 있는 것만)."""
    return sorted({k for k, v in ROLE_TO_FILE.items() if (STORYLINES_DIR / v).exists()})


def reload_configs() -> None:
    """캐시 초기화 — JSON 편집 후 호출."""
    _CONFIG_CACHE.clear()


# ─── 사용자 → 스토리라인 직무 매핑 ──────────────────────────────────────
def role_detect(user: Optional[dict]) -> dict:
    """로그인한 사용자의 user dict에서 스토리라인 role ID 결정.

    Returns:
        {
            "role": "cp" | "pd_program" | "pd_schedule" | None,
            "available": bool,                  # JSON + 필요 매핑이 있는지
            "missing_setup": [str],             # 부족한 셋업 사항
            "user_context": {...}               # entry 호출 시 사용할 컨텍스트
        }
    """
    if not user:
        return {
            "role": None,
            "available": False,
            "missing_setup": ["로그인이 필요합니다."],
            "user_context": {},
        }

    korean_role = (user.get("role") or "").strip()
    role_id = KOREAN_ROLE_TO_ID.get(korean_role) or KOREAN_ROLE_TO_ID.get(korean_role.lower())

    missing: list[str] = []
    if not role_id:
        missing.append(
            f"사용자의 role '{korean_role}'에 대응하는 스토리라인이 없습니다. "
            f"현재 지원: CP, 제작PD, 편성PD"
        )
        return {
            "role": None,
            "available": False,
            "missing_setup": missing,
            "user_context": {"user_name": user.get("name", "")},
        }

    # CP는 channel 매핑이 필요
    user_context = {"user_name": user.get("name", "") or user.get("login_id", "사용자")}
    if role_id == "cp":
        channel = _derive_cp_channel(user)
        if not channel:
            missing.append(
                "CP→채널 매핑이 등록되지 않았습니다. "
                "프로필에서 담당 채널(파워FM 또는 러브FM)을 선택해 주세요."
            )
        else:
            user_context["channel_name"] = channel

    return {
        "role": role_id,
        "available": not missing,
        "missing_setup": missing,
        "user_context": user_context,
    }


def _derive_cp_channel(user: dict) -> Optional[str]:
    """CP 사용자의 채널 결정.

    우선순위:
    1. user["channel"] 명시 필드
    2. user["my_programs"] 첫 프로그램 코드의 prefix → 채널
    3. None (사용자 본인이 등록 필요)
    """
    if user.get("channel"):
        return user["channel"]

    for code in _parse_my_programs(user):
        ch = PROGRAM_PREFIX_TO_CHANNEL.get(code[0].upper())
        if ch in ("파워FM", "러브FM"):
            return ch
    return None


def _parse_my_programs(user: dict) -> list[str]:
    """user.my_programs를 list[str]로 표준화. SQLite는 JSON 문자열로 저장."""
    my = user.get("my_programs") or []
    if isinstance(my, str):
        try:
            my = json.loads(my)
        except (ValueError, TypeError):
            my = []
    return [c for c in (my or []) if isinstance(c, str) and c]


def _derive_pd_primary_program(user: dict) -> Optional[str]:
    """제작PD가 가장 우선시하는 프로그램 코드 (my_programs[0])."""
    codes = _parse_my_programs(user)
    return codes[0].upper() if codes else None


# ─── 데이터 소스 — Splunk 우선, CSV는 fallback ────────────────────
# raas_server가 부팅 시 timeline provider 등록 (get_cached_timeline)
# 이게 없으면 (CLI/테스트 환경) CSV 직접 읽기로 폴백.
_TIMELINE_PROVIDER = None  # type: ignore


def set_timeline_provider(fn) -> None:
    """raas_server.py에서 호출: STORY.set_timeline_provider(get_cached_timeline).

    Splunk REST API → CSV 보충 통합 파이프라인을 스토리라인이 그대로 쓰도록.
    """
    global _TIMELINE_PROVIDER
    _TIMELINE_PROVIDER = fn


# CSV fallback 캐시 (provider 미설정 또는 실패 시만 사용)
_KPI_CACHE: list[dict] | None = None
_KPI_CACHE_MTIME: float | None = None


def _kpi_rows() -> list[dict]:
    """KPI 행 목록.

    우선순위:
        1) timeline provider — 일반 채팅·브리핑과 동일한 Splunk 파이프라인
           ({PGM_CODE: {DATE: row}} → flat row list 변환)
        2) data/raas_kpi_latest.csv — provider 미설정·실패 시 폴백 (mtime 캐시)
    """
    # (1) timeline provider — Splunk 실시간 + CSV 보충
    if _TIMELINE_PROVIDER is not None:
        try:
            timeline = _TIMELINE_PROVIDER()
            if timeline:
                flat: list[dict] = []
                for code, date_rows in timeline.items():
                    if not isinstance(date_rows, dict):
                        continue
                    for date, row in date_rows.items():
                        if not isinstance(row, dict):
                            continue
                        # PGM_CODE/DATE 보장 (드물게 누락 가능)
                        row_out = {**row}
                        row_out["PGM_CODE"] = row.get("PGM_CODE") or code
                        row_out["DATE"] = row.get("DATE") or date
                        flat.append(row_out)
                if flat:
                    return flat
        except Exception as e:
            print(f"[storyline] timeline provider 실패 → CSV fallback: {e}")

    # (2) CSV fallback (mtime 캐시)
    global _KPI_CACHE, _KPI_CACHE_MTIME
    if not KPI_CSV.exists():
        return []
    mtime = KPI_CSV.stat().st_mtime
    if _KPI_CACHE is not None and _KPI_CACHE_MTIME == mtime:
        return _KPI_CACHE
    with open(KPI_CSV, encoding="utf-8-sig") as f:
        _KPI_CACHE = list(csv.DictReader(f))
    _KPI_CACHE_MTIME = mtime
    return _KPI_CACHE


def _load_program_latest_row(code: str) -> Optional[dict]:
    """프로그램 코드의 가장 최근일 KPI row."""
    rows = [r for r in _kpi_rows() if r.get("PGM_CODE", "").upper() == code.upper()]
    if not rows:
        return None
    rows.sort(key=lambda r: r.get("DATE", ""), reverse=True)
    return rows[0]


def _load_program_history(code: str, lookback_days: int = 60) -> list[dict]:
    """프로그램 코드의 최근 N일 KPI rows (오름차순)."""
    rows = [r for r in _kpi_rows() if r.get("PGM_CODE", "").upper() == code.upper()]
    rows.sort(key=lambda r: r.get("DATE", ""))
    return rows[-lookback_days:] if lookback_days else rows


# 요일 한국어
_WEEKDAY_KO = ["월", "화", "수", "목", "금", "토", "일"]


def _weekday_ko(date_str: str) -> str:
    """'YYYY/MM/DD' → '월'~'일'."""
    try:
        from datetime import datetime
        d = datetime.strptime(date_str.replace("-", "/"), "%Y/%m/%d")
        return _WEEKDAY_KO[d.weekday()]
    except (ValueError, TypeError):
        return ""


# ─── 원인 분석 온톨로지 로더 (RDF/Turtle, D-016) ─────────────────
# raas_onto/raas_ontology_kpi.ttl + raas_ontology_cause.ttl 결합
# SPARQL로 4축·해석 규칙·적재 힌트·우선순위 조회
_RDF_GRAPH = None


def _load_rdf_graph():
    """KPI + cause 온톨로지 결합 그래프 캐시 로드."""
    global _RDF_GRAPH
    if _RDF_GRAPH is not None:
        return _RDF_GRAPH
    try:
        import rdflib
    except ImportError:
        print("[ontology] rdflib 미설치 — 원인 분석 비활성")
        return None
    g = rdflib.Graph()
    for fname in ("raas_ontology_kpi.ttl", "raas_ontology_cause.ttl", "raas_ontology_calendar.ttl"):
        p = ONTO_DIR / fname
        if p.exists():
            try:
                g.parse(p, format="turtle")
            except Exception as e:
                print(f"[ontology] {fname} 파싱 실패: {e}")
    _RDF_GRAPH = g
    return g


def _query_decompositions() -> list[dict]:
    """4축을 priority 순으로 반환. 각 축의 사용 메트릭 + 해석 규칙 + 적재 힌트 포함."""
    g = _load_rdf_graph()
    if g is None:
        return []

    # 4축 메타 조회
    q_axes = """
    PREFIX raas: <http://raas.sbs.co.kr/onto#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    SELECT ?ax ?label ?prio ?purpose ?availLabel ?formula WHERE {
        ?ax a raas:CauseDecomposition ;
            rdfs:label ?label ;
            raas:priority ?prio ;
            raas:purpose ?purpose ;
            raas:dataAvailability ?avail .
        ?avail rdfs:label ?availLabel .
        OPTIONAL { ?ax raas:formula ?formula }
        FILTER(LANG(?label) = "ko")
        FILTER(LANG(?availLabel) = "ko")
    } ORDER BY ?prio
    """
    axes: list[dict] = []
    for r in g.query(q_axes):
        ax_id = str(r.ax).split("#")[-1]
        axes.append({
            "id": ax_id,
            "label": str(r.label),
            "priority": int(r.prio),
            "purpose": str(r.purpose),
            "availability": str(r.availLabel),
            "formula": str(r.formula) if r.formula else None,
            "available": "적재 완료" in str(r.availLabel),
        })

    # 각 축의 적재 힌트 (세그먼트용)
    q_hints = """
    PREFIX raas: <http://raas.sbs.co.kr/onto#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    SELECT ?ax ?key ?label ?spl WHERE {
        ?ax raas:hasIngestHint ?hint .
        ?hint raas:metricKey ?key ;
              raas:splunkSPL ?spl ;
              rdfs:label ?label .
        FILTER(LANG(?label) = "ko")
    }
    """
    hints_by_ax: dict = {}
    for r in g.query(q_hints):
        ax_id = str(r.ax).split("#")[-1]
        hints_by_ax.setdefault(ax_id, []).append({
            "key": str(r.key),
            "label": str(r.label),
            "splunk_spl": str(r.spl),
        })

    for ax in axes:
        ax["ingest_hints"] = hints_by_ax.get(ax["id"], [])

    return axes


def _query_interpretation_rules(decomp_local: str) -> dict:
    """특정 축의 해석 규칙을 trigger → interpretation 매핑으로 반환."""
    g = _load_rdf_graph()
    if g is None:
        return {}
    q = f"""
    PREFIX raas: <http://raas.sbs.co.kr/onto#>
    SELECT ?trig ?interp WHERE {{
        raas:{decomp_local} raas:hasInterpretationRule ?rule .
        ?rule raas:trigger ?trig ;
              raas:interpretation ?interp .
        FILTER(LANG(?interp) = "ko")
    }}
    """
    return {str(r.trig): str(r.interp) for r in g.query(q)}


# ─── Claude API 다듬기 헬퍼 ─────────────────────────────────────
_NO_HRULE_RULE = (
    "마크다운 가로줄(`---`, `***`, `___`)을 절대 사용하지 마세요. "
    "섹션 사이는 빈 줄 1개로만 구분합니다."
)


def _strip_hrules(text: str) -> str:
    """LLM이 임의로 삽입한 가로줄(---/***/___) 라인을 제거 + 과도한 빈 줄 정리."""
    if not text:
        return text
    # 단독 가로줄 라인 제거
    text = re.sub(r"^[ \t]*[-*_]{3,}[ \t]*$", "", text, flags=re.MULTILINE)
    # 3개 이상 연속 빈 줄 → 2개로 줄임
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


# 스토리라인 1회 advance 동안 _llm_polish가 쓴 토큰 누적기 (서버 단일스레드 전제)
_POLISH_USAGE = {"input": 0, "output": 0}


def _reset_polish_usage():
    _POLISH_USAGE["input"] = 0
    _POLISH_USAGE["output"] = 0


def _get_polish_usage() -> dict:
    return {"input_tokens": _POLISH_USAGE["input"] or None,
            "output_tokens": _POLISH_USAGE["output"] or None}


def _llm_polish(raw_text: str, instruction: Optional[str] = None,
                max_tokens: int = 800) -> str:
    """raw_text를 자연스럽게 다듬어 반환. Claude API 실패 시 원본 그대로."""
    if not raw_text or not raw_text.strip():
        return raw_text or ""
    try:
        import os
        import anthropic
        api_key = os.getenv("ANTHROPIC_API_KEY")
        if not api_key:
            return _strip_hrules(raw_text)
        client = anthropic.Anthropic(api_key=api_key)
        instr = instruction or (
            "다음 텍스트를 자연스러운 한국어로 다듬어 주세요. "
            "마크다운(**굵게**, 표) 구조는 그대로 유지하고, 결과만 출력하세요."
        )
        # 모든 polish 호출에 공통 적용되는 가로줄 금지 규칙
        instr = instr.rstrip() + "\n- " + _NO_HRULE_RULE
        # raw 텍스트 구분에 fence를 사용 (가로줄 대신)
        prompt = f"{instr}\n\n[원본 시작]\n{raw_text}\n[원본 끝]"
        # 다듬기는 빠른 응답 우선 — Haiku 사용
        model = os.getenv("CLAUDE_POLISH_MODEL") \
                or os.getenv("CLAUDE_MODEL") \
                or "claude-haiku-4-5-20251001"
        resp = client.messages.create(
            model=model,
            max_tokens=max_tokens,
            messages=[{"role": "user", "content": prompt}],
        )
        try:  # 토큰 사용량 누적 (advance가 수집 → 히스토리 기록)
            _u = getattr(resp, "usage", None)
            if _u:
                _POLISH_USAGE["input"] += getattr(_u, "input_tokens", 0) or 0
                _POLISH_USAGE["output"] += getattr(_u, "output_tokens", 0) or 0
        except Exception:
            pass
        text = (resp.content[0].text or "").strip() if resp.content else ""
        # 모델이 [원본 시작/끝] 마커를 포함했을 때 정리
        text = re.sub(r"^\[원본 시작\]\s*", "", text)
        text = re.sub(r"\s*\[원본 끝\]\s*$", "", text)
        text = _strip_hrules(text)
        return text or _strip_hrules(raw_text)
    except Exception as e:
        print(f"[llm_polish] failed (원본 사용): {e}")
        return _strip_hrules(raw_text)


# ─── 흐름 분해 / 코호트 / Stickiness 계산 (cause_analysis 슬롯용) ──
def _safe_float(v) -> Optional[float]:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


# ─── 같은 요일 baseline 헬퍼 ───────────────────────────────────
def _date_weekday_int(date_str: str) -> Optional[int]:
    """'YYYY/MM/DD' 또는 'YYYY-MM-DD' → 0(월)~6(일)."""
    try:
        from datetime import datetime
        dt = datetime.strptime((date_str or "").replace("-", "/"), "%Y/%m/%d")
        return dt.weekday()
    except (ValueError, TypeError):
        return None


def _weekday_or_simple_avg(history: Optional[list], value_fn,
                           target_weekday: Optional[int],
                           min_samples: int = 3,
                           fallback_window: int = 28) -> tuple[Optional[int], str, int]:
    """같은 요일만 골라 평균. 표본 부족하면 직전 N일 단순 평균으로 폴백.

    Returns:
        (평균값, source: 'same_weekday' or 'simple_avg', 표본수)
    """
    if not history:
        return None, "none", 0

    # 1) 같은 요일만
    if target_weekday is not None:
        same_vals = []
        for r in history:
            if _date_weekday_int(r.get("DATE", "")) != target_weekday:
                continue
            v = value_fn(r)
            if v is not None:
                same_vals.append(v)
        if len(same_vals) >= min_samples:
            return round(sum(same_vals) / len(same_vals)), "same_weekday", len(same_vals)

    # 2) 단순 평균 폴백
    fb_vals = []
    for r in history[-fallback_window:]:
        v = value_fn(r)
        if v is not None:
            fb_vals.append(v)
    if fb_vals:
        return round(sum(fb_vals) / len(fb_vals)), "simple_avg", len(fb_vals)
    return None, "none", 0


def _compute_wow_change(history: list, today_row: dict) -> Optional[float]:
    """전주 동요일 대비 변화율(%). 7일 전 같은 요일 row의 dau로 계산.

    history는 같은 PGM_CODE의 오름차순 행들. today_row는 history[-1] 또는 별도 어제 row.
    7일 전 행이 없거나 dau가 없으면 None.
    """
    today_date = today_row.get("DATE", "")
    today_dau = _safe_float(today_row.get("dau"))
    if not today_date or today_dau is None or today_dau == 0:
        return None
    from datetime import datetime, timedelta
    try:
        dt = datetime.strptime(today_date.replace("-", "/"), "%Y/%m/%d")
    except (ValueError, TypeError):
        return None
    target = (dt - timedelta(days=7)).strftime("%Y/%m/%d")
    target_dash = (dt - timedelta(days=7)).strftime("%Y-%m-%d")
    for r in history:
        d = r.get("DATE", "")
        if d == target or d == target_dash:
            prev = _safe_float(r.get("dau"))
            if prev is not None and prev > 0:
                return round((today_dau - prev) / prev * 100, 1)
            return None
    return None


def _compute_cp_anchor_history_for(code: str) -> list:
    """1_anchor TOP 선정용 — 한 프로그램의 직전 14일+ 행. WoW 계산에 필요한 7일 전 행 보장."""
    return _load_program_history(code, lookback_days=21)


def _compute_weekday_pattern_check(row: dict, history: Optional[list]) -> dict:
    """이번 WoW(전주 동요일 대비) 변화율이 평소 같은 요일의 WoW 분포 내인지 사전 진단.

    - 같은 요일의 과거 행에서 각각 그 시점의 WoW를 계산 → 분포
    - 표본 ≥ 3 — 평균·범위·표준편차 계산
    - |z| ≤ 1.5 → 통상 범위 (자연스러운 반복 변화)
    - 그 외 → 평소를 벗어남
    """
    import math
    target_wd = _date_weekday_int(row.get("DATE", ""))
    current = _compute_wow_change(history or [], row)
    if target_wd is None or current is None or not history:
        return {"ok": False, "reason": "데이터 부족"}

    # 어제 행 제외 + 같은 요일에서 각각 WoW 계산
    history_excl = history[:-1] if len(history) > 1 else []
    wows = []
    for r in history_excl:
        if _date_weekday_int(r.get("DATE", "")) != target_wd:
            continue
        w = _compute_wow_change(history_excl, r)
        if w is not None:
            wows.append(w)

    if len(wows) < 3:
        return {"ok": False, "reason": f"같은 요일 WoW 표본 {len(wows)}건"}

    avg = sum(wows) / len(wows)
    var = sum((c - avg) ** 2 for c in wows) / len(wows)
    std = math.sqrt(var) if var > 0 else 0
    z = (current - avg) / std if std > 0 else 0
    weekday_ko = _WEEKDAY_KO[target_wd]

    return {
        "ok": True,
        "current_chg": round(current, 1),
        "avg_chg": round(avg, 1),
        "std_chg": round(std, 1),
        "min_chg": round(min(wows), 1),
        "max_chg": round(max(wows), 1),
        "z_score": round(z, 2),
        "is_within_normal_range": abs(z) <= 1.5,
        "weekday_ko": weekday_ko,
        "sample_size": len(wows),
        "metric_name": "전주 동요일 대비",
    }


def _detect_flow_pattern(history: Optional[list], driver_key: str,
                         baseline: Optional[float]) -> str:
    """biggest driver의 패턴:
        - 'one_off'    : 어제만 평상시 대비 크게 벗어남 → 단발성 이벤트 추정
        - 'sustained'  : 직전 3일 중 2일 이상 같은 방향 → 구조적 변화 추정
        - 'normal'     : 변동 미미

    임계값: |편차| / baseline > 10%
    """
    if not history or len(history) < 4 or not driver_key:
        return "normal"
    if baseline is None or baseline == 0:
        return "normal"

    def _val(row):
        if driver_key == "new":   return _safe_float(row.get("new"))
        if driver_key == "react": return _safe_float(row.get("react"))
        if driver_key == "churn":
            dp = _safe_float(row.get("dau_prev"))
            cr = _safe_float(row.get("churn_rate"))
            if dp is None or cr is None: return None
            return round(dp * cr / 100)
        return None

    y = _val(history[-1])
    if y is None:
        return "normal"
    y_dev = (y - baseline) / max(abs(baseline), 1)
    if abs(y_dev) < 0.10:
        return "normal"

    # 직전 3일에서 같은 방향으로 10% 이상 벗어난 일수
    same_dir = 0
    for r in history[-4:-1]:
        v = _val(r)
        if v is None: continue
        v_dev = (v - baseline) / max(abs(baseline), 1)
        if v_dev * y_dev > 0 and abs(v_dev) >= 0.10:
            same_dir += 1
    return "sustained" if same_dir >= 2 else "one_off"


def _flow_hypothesis(driver_key: str, value: int, baseline: float, pattern: str) -> str:
    """biggest driver + pattern → 한 줄 추정 의견 (data-derivable만, normal이면 빈 문자열)."""
    if pattern == "normal" or baseline is None:
        return ""
    increased = value > baseline
    if driver_key == "new":
        if pattern == "one_off":
            return ("어제 단발성 변화 — 외부 노출·홍보 이벤트 등 일회성 요인 가능성" if increased
                    else "어제 단발성 변화 — 유입 채널의 일시적 이슈 가능성")
        return ("최근 며칠 이어진 흐름 — 외부 노출·마케팅의 구조적 변화 가능성" if increased
                else "최근 며칠 이어진 흐름 — 외부 노출 감소 등 구조적 요인 가능성")
    if driver_key == "react":
        if pattern == "one_off":
            return ("어제 단발성 변화 — 푸시·특집·미디어 노출 등 일회성 복귀 트리거 가능성" if increased
                    else "어제 단발성 변화 — 평소 복귀 트리거(푸시 등) 부재 가능성")
        return ("최근 며칠 이어진 흐름 — 콘텐츠 강화에 의한 복귀 추세 가능성" if increased
                else "최근 며칠 이어진 흐름 — 복귀 유도력 약화 가능성")
    if driver_key == "churn":
        # 이탈은 값 클수록 나쁨
        if pattern == "one_off":
            return ("어제 단발성 변화 — 어제 콘텐츠·서비스 일시적 이슈 가능성" if increased
                    else "어제 단발성 변화 — 특별 회차·게스트 효과로 일시적 이탈 감소 가능성")
        return ("최근 며칠 이어진 흐름 — 콘텐츠 매력도·UX 측면의 누적 이슈 가능성" if increased
                else "최근 며칠 이어진 흐름 — 콘텐츠 강화·UX 개선의 누적 효과 가능성")
    return ""


def _compute_flow_decomposition(row: dict, history: Optional[list] = None) -> dict:
    """유입/이탈/복귀 분해 + 평상시(직전 28일 평균) 대비.

    Δdau ≈ new + react − (dau_prev × churn_rate / 100)
    이탈은 추정이며 등식이 정확히 안 맞는 차이는 주로 복귀 사용자 인식 안정화 이슈에서 발생.
    """
    dau      = _safe_float(row.get("dau"))
    dau_prev = _safe_float(row.get("dau_prev"))
    new      = _safe_float(row.get("new"))
    react    = _safe_float(row.get("react"))
    churn_r  = _safe_float(row.get("churn_rate"))
    if dau_prev is None or new is None or react is None or churn_r is None:
        return {"ok": False, "missing": ["new/react/churn_rate/dau_prev 중 결측"]}
    churn_count = round(dau_prev * churn_r / 100)
    delta = (dau - dau_prev) if (dau is not None and dau_prev) else None

    # 평상시 평균 — 같은 요일 평균 우선, 표본 부족 시 28일 단순 평균 폴백
    baseline = {}
    if history and len(history) > 1:
        history_excl = history[:-1]  # 어제 행 제외
        target_wd = _date_weekday_int(row.get("DATE", ""))

        def _churn_val(r):
            dp_ = _safe_float(r.get("dau_prev"))
            cr_ = _safe_float(r.get("churn_rate"))
            if dp_ is None or cr_ is None: return None
            return round(dp_ * cr_ / 100)

        new_avg, src, n_new = _weekday_or_simple_avg(
            history_excl, lambda r: _safe_float(r.get("new")), target_wd)
        react_avg, _, n_react = _weekday_or_simple_avg(
            history_excl, lambda r: _safe_float(r.get("react")), target_wd)
        churn_avg, _, n_churn = _weekday_or_simple_avg(
            history_excl, _churn_val, target_wd)

        if new_avg is not None:   baseline["new_avg"] = new_avg
        if react_avg is not None: baseline["react_avg"] = react_avg
        if churn_avg is not None: baseline["churn_avg"] = churn_avg
        baseline["baseline_source"] = src  # 'same_weekday' 또는 'simple_avg'
        baseline["sample_size"] = min(filter(None, [n_new, n_react, n_churn]), default=0)

    # 등식 모순(=복귀 풀 안정화 이슈) 크기
    expected_delta = int(new) + int(react) - int(churn_count)
    actual_delta = int(delta) if delta is not None else expected_delta
    discrepancy = actual_delta - expected_delta

    # 가장 큰 변동 요인 (DAU 변화에 미치는 영향 절대값 기준)
    biggest_driver = None
    if baseline:
        contribs = []
        if baseline.get("new_avg") is not None:
            contribs.append(("신규", "new", int(new),
                             int(new) - baseline["new_avg"]))
        if baseline.get("react_avg") is not None:
            contribs.append(("복귀", "react", int(react),
                             int(react) - baseline["react_avg"]))
        if baseline.get("churn_avg") is not None:
            # 이탈은 값이 클수록 DAU 감소 → 영향력 부호 반전해서 비교
            churn_diff = int(churn_count) - baseline["churn_avg"]
            contribs.append(("이탈", "churn", int(churn_count), -churn_diff))
        if contribs:
            big = max(contribs, key=lambda c: abs(c[3]))
            bl_val = baseline.get(big[1] + "_avg")
            biggest_driver = {
                "label": big[0],
                "key": big[1],
                "value": big[2],
                "baseline": bl_val,
                "diff": (big[2] - bl_val) if bl_val is not None else 0,
                "pattern": _detect_flow_pattern(history, big[1], bl_val),
            }

    return {
        "ok": True,
        "delta_dau": int(delta) if delta is not None else None,
        "new": int(new),
        "react": int(react),
        "churn_estimate": int(churn_count),
        "dau_prev": int(dau_prev),
        "dau": int(dau) if dau is not None else None,
        "baseline": baseline,
        "discrepancy": int(discrepancy),
        "biggest_driver": biggest_driver,
    }


def _avg_col(rows: list, col: str, target_wd: Optional[int] = None) -> Optional[float]:
    """history rows에서 col 평균. target_wd 지정 시 같은 요일만 (표본<2면 전체 폴백)."""
    def collect(wd):
        out = []
        for r in rows or []:
            if wd is not None and _date_weekday_int(r.get("DATE", "")) != wd:
                continue
            v = _safe_float(r.get(col))
            if v is not None:
                out.append(v)
        return out
    vals = collect(target_wd)
    if len(vals) < 2 and target_wd is not None:
        vals = collect(None)
    return (sum(vals) / len(vals)) if vals else None


# 기간별 비율 컬럼: (신규비율, 복귀율, 이탈율)
_RATE_LENS_COLS = {
    "day":  ("new_share",      "react_rate",      "churn_rate"),
    "week": ("new_week_share", "react_rate_week", "churn_rate_week"),
    "mon":  ("new_mon_share",  "react_rate_mon",  "churn_rate_mon"),
}


def _rate_lens_lines(row: dict, history: Optional[list], period: str) -> list:
    """신규비율·복귀율·이탈율을 '평상시 대비 편차(%p)'로 제시.

    카운트 항등식(ΔDAU=신규+복귀−이탈)은 복귀 인식·중복제거 차이로 정확히 닫히지
    않으므로(특히 월간), 누수 영향이 적은 '비율 × 베이스라인 대비'를 견고한 보조 축으로 둔다.
      - day  : 같은 요일 평균 기준 (요일 계절성 보정 — 라디오 핵심)
      - week/mon : 직전 기간들의 평균 기준
    가장 크게 벗어난 레버를 원인 후보로 한 줄 덧붙인다.
    """
    cols = _RATE_LENS_COLS.get(period)
    if not cols:
        return []
    new_col, react_col, churn_col = cols
    target_wd = _date_weekday_int(row.get("DATE", "")) if period == "day" else None
    pool = (history or [])[:-1] if history else []
    base_label = {"day": "평상시 같은 요일", "week": "평상시(최근 주)",
                  "mon": "평상시(최근 월)"}[period]

    specs = [("신규비율", new_col, False), ("복귀율", react_col, False),
             ("이탈율", churn_col, True)]
    body, devs = [], []
    for label, col, is_churn in specs:
        cur = _safe_float(row.get(col))
        if cur is None:
            continue
        base = _avg_col(pool, col, target_wd)
        if base is None:
            body.append(f"  - **{label}** {cur:.1f}% (평상시 비교 데이터 부족)")
            continue
        diff = cur - base
        arrow = "↑" if diff > 0 else ("↓" if diff < 0 else "·")
        body.append(
            f"  - **{label}** {cur:.1f}% — {base_label} {base:.1f}% 대비 **{diff:+.1f}%p {arrow}**"
        )
        # 영향 방향: 신규·복귀↑ → DAU↑ / 이탈↑ → DAU↓
        devs.append((label, abs(diff), (-diff if is_churn else diff)))
    if not body:
        return []
    out = ["**비율 관점 (평상시 대비)**"] + body
    dom = max(devs, key=lambda d: d[1]) if devs else None
    if dom and dom[1] >= 0.5:   # 0.5%p 이상 벗어났을 때만 원인 후보로
        sign = "끌어올리는" if dom[2] > 0 else "끌어내리는"
        out.append(
            f"  → 비율상 가장 크게 벗어난 건 **{dom[0]}**으로, "
            f"활성사용자를 {sign} 방향으로 작용했습니다."
        )
    return out


# 기간별 레벨 분해 설정: (DAU컬럼, 신규컬럼, 복귀컬럼, 비교offset일, 라벨, 이탈율컬럼, 이탈base컬럼)
_FLOW_PERIOD = {
    "day":  ("dau", "new",      "react",      7,  "전주 동요일", "churn_rate",      "dau_d2"),
    "week": ("wau", "new_week", "react_week", 7,  "전주",        "churn_rate_week", "wau_prev"),
    "mon":  ("mau", "new_mon",  "react_mon",  30, "전월",        "churn_rate_mon",  "mau_prev"),
}


def _date_minus(date_str: str, days: int) -> Optional[str]:
    from datetime import datetime as _dt, timedelta as _td
    try:
        return (_dt.strptime(date_str.replace("-", "/"), "%Y/%m/%d") - _td(days=days)).strftime("%Y/%m/%d")
    except Exception:
        return None


def _flow_components(row: Optional[dict], period: str) -> Optional[dict]:
    """행에서 DAU = 신규 + 복귀 + 유지 구성 추출. 유지 = DAU − 신규 − 복귀 (잔차)."""
    if not row:
        return None
    cfg = _FLOW_PERIOD[period]
    dau   = _safe_float(row.get(cfg[0]))
    new   = _safe_float(row.get(cfg[1]))
    react = _safe_float(row.get(cfg[2]))
    if dau is None or new is None or react is None:
        return None
    return {"dau": dau, "new": new, "react": react, "retain": dau - new - react}


def _wow_flow_decomp(row: dict, history: Optional[list], period: str) -> Optional[dict]:
    """직전 동일 기간(전주동요일/전주/전월) 대비 레벨 구조분해.

    ΔDAU = Δ신규 + Δ복귀 + Δ유지 — 유지가 잔차라 정의상 100% 닫힘.
    이탈(직전 기간 base × 이탈율)은 '유지 형성' 설명용 참고치로만 첨부(근사).
    """
    cfg = _FLOW_PERIOD[period]
    cur = _flow_components(row, period)
    if not cur:
        return {"ok": False, "period": period, "label": cfg[4],
                "reason": "복귀(react) 등 구성 데이터 미집계"}
    cmp_date = _date_minus(row.get("DATE", ""), cfg[3])
    cmp_row = None
    if cmp_date and history:
        for r in history:
            if (r.get("DATE") or "").replace("-", "/") == cmp_date:
                cmp_row = r
                break
    prev = _flow_components(cmp_row, period)
    out = {"ok": True, "period": period, "label": cfg[4], "cur": cur, "prev": prev}
    if prev:
        d = {k: cur[k] - prev[k] for k in ("new", "react", "retain", "dau")}
        out["delta"] = d
        out["dominant"] = max(("new", "react", "retain"), key=lambda k: abs(d[k]))
    # 이탈 참고치 (직전 기간 active × 이탈율) + 비교 기간(전주동요일/전주/전월) 이탈율
    churn = _safe_float(row.get(cfg[5]))
    base  = _safe_float(row.get(cfg[6]))
    if churn is not None:
        out["churn_rate"] = churn
        if base is not None:
            out["churn_est"] = round(base * churn / 100)
    if cmp_row is not None:
        churn_prev = _safe_float(cmp_row.get(cfg[5]))
        if churn_prev is not None:
            out["churn_rate_prev"] = churn_prev
    return out


def _flow_decomp_lines(decomp: Optional[dict]) -> list:
    """레벨 분해 결과 → 마크다운 본문(헤더 제외 — 호출부에서 헤더 출력)."""
    if not decomp or not decomp.get("ok"):
        label  = (decomp or {}).get("label", "")
        reason = (decomp or {}).get("reason", "데이터 부족")
        return [f"- ⚠ {label} 흐름 분해 보류 — {reason} (적재되면 자동 표시)"]
    label = decomp["label"]
    cur = decomp["cur"]
    names = {"new": "신규", "react": "복귀", "retain": "유지"}

    def _i(v):  return f"{int(round(v)):,}"
    def _d(v):  return f"{int(round(v)):+,}"

    lines = []
    prev = decomp.get("prev")
    if prev and decomp.get("delta"):
        dd = decomp["delta"]
        wow = (dd["dau"] / prev["dau"] * 100) if prev["dau"] else None
        lines.append(f"| 구성 | {label} | 이번 | Δ |")
        lines.append("|---|---|---|---|")
        for k in ("new", "react", "retain"):
            lines.append(f"| {names[k]} | {_i(prev[k])} | {_i(cur[k])} | {_d(dd[k])} |")
        dau_d = _d(dd["dau"]) + (f" ({wow:+.1f}%)" if wow is not None else "")
        lines.append(f"| **활성사용자** | {_i(prev['dau'])} | {_i(cur['dau'])} | **{dau_d}** |")
        dom = decomp.get("dominant")
        if dom:
            dvi = int(round(dd[dom]))
            dau_d = round(dd["dau"])
            arrow = "끌어올린" if dvi > 0 else "끌어내린"
            # 상쇄 요인: 지배항과 반대 부호이면서 |Δ|가 지배항의 50% 이상인 항목
            opp = None
            for k in ("new", "react", "retain"):
                if k == dom or dd[k] == 0:
                    continue
                if (dd[k] > 0) != (dvi > 0) and abs(dd[k]) >= abs(dvi) * 0.5:
                    if opp is None or abs(dd[k]) > abs(dd[opp]):
                        opp = k
            msg = (f"→ {label} 대비 활성사용자 변화의 주된 변동 요인은 "
                   f"**{names[dom]}**입니다 (**{dvi:+,}명**) — 활성사용자를 {arrow} 방향.")
            if opp is not None:
                # 상쇄가 있으면 % 대신 상쇄 요인을 명시 (순변화 대비 %가 오도될 수 있음)
                msg += f" 다만 **{names[opp]}**({int(round(dd[opp])):+,})이 이를 상당 부분 상쇄했습니다."
            elif dau_d != 0 and (dvi > 0) == (dau_d > 0):
                share = abs(dvi) / abs(dau_d) * 100
                if share <= 110:   # 상쇄 없을 때만, 100% 부근 이하에서 비중 표기
                    msg = msg[:-1] + f", 전체 변화의 약 {share:.0f}%."
            lines.append("")
            lines.append(msg)
    else:
        lines.append(
            f"- 현재 구성: 신규 {_i(cur['new'])} · 복귀 {_i(cur['react'])} · "
            f"유지 {_i(cur['retain'])} (= 활성사용자 {_i(cur['dau'])})"
        )
        lines.append(f"- ⚠ {label} 비교 데이터가 없어 변화 분해는 생략")
    if decomp.get("churn_rate") is not None:
        c = decomp["churn_rate"]
        parts = []
        cp = decomp.get("churn_rate_prev")
        if cp is not None:
            parts.append(f"{label} {cp:.1f}%, {c - cp:+.1f}%p")
        ce = decomp.get("churn_est")
        if ce is not None:
            parts.append(f"약 {ce:,}명")
        extra = f" ({', '.join(parts)})" if parts else ""
        lines.append(f"[mini]참고: 이탈율 {c:.1f}%{extra} — 유지 형성 맥락(원인 귀속 아님)[/mini]")
    return lines


def _compute_programming_impact(row: dict, history: Optional[list]) -> dict:
    """편성 영향 분해 — 어제 vs 평소 같은 요일 (game / corner / live / view_radio).

    필드:
        guestname / daily_corner / weekly_corner / live_yn / view_radio_yn
    """
    target_wd = _date_weekday_int(row.get("DATE", ""))
    if target_wd is None or not history:
        return {"ok": False, "reason": "데이터 부족"}
    # 같은 요일 표본 (어제 제외)
    same_wd = [r for r in history[:-1]
               if _date_weekday_int(r.get("DATE", "")) == target_wd]
    if len(same_wd) < 2:
        return {"ok": False, "reason": f"같은 요일 표본 {len(same_wd)}건 (≥2 필요)"}

    yesterday = {
        "guestname": (row.get("guestname") or "").strip(),
        "daily_corner": (row.get("daily_corner") or "").strip(),
        "weekly_corner": (row.get("weekly_corner") or "").strip(),
        "live_yn": (row.get("live_yn") or "").strip(),
        "view_radio_yn": (row.get("view_radio_yn") or "").strip(),
    }

    from collections import Counter
    def _most_common(field: str) -> tuple[str, int]:
        vals = [(r.get(field) or "").strip() for r in same_wd]
        non_empty = [v for v in vals if v]
        if not non_empty:
            return "", 0
        c = Counter(non_empty)
        most = c.most_common(1)[0]
        return most[0], most[1]

    differences: list[dict] = []
    triggers: list[str] = []
    wd_ko = _WEEKDAY_KO[target_wd]

    # 게스트 — 유무 자체를 우선 (사용자 요청: 게스트는 신규 유입에 큰 영향)
    yest_has_guest = bool(yesterday["guestname"])
    prior_with_guest = sum(1 for r in same_wd
                           if (r.get("guestname") or "").strip())
    prior_guest_rate = prior_with_guest / len(same_wd)  # 0.0~1.0

    if yest_has_guest and prior_guest_rate < 0.5:
        # 평소엔 게스트 드문데 어제 출연 → 특별 게스트
        differences.append({
            "label": "게스트",
            "yesterday": f"출연 ({yesterday['guestname']})",
            "common": (f"평소 {wd_ko}요일은 대체로 무게스트"
                       f" ({int((1-prior_guest_rate)*100)}% 표본)"),
            "impact": "high",
            "category": "guest_appear",
        })
        triggers.append("special_guest")
    elif (not yest_has_guest) and prior_guest_rate >= 0.7:
        # 평소엔 게스트 있는데 어제 없음
        differences.append({
            "label": "게스트",
            "yesterday": "부재",
            "common": (f"평소 {wd_ko}요일은 대체로 출연"
                       f" ({int(prior_guest_rate*100)}% 표본)"),
            "impact": "high",
            "category": "guest_absent",
        })
        triggers.append("no_guest_today")
    elif yest_has_guest:
        # 어제·평소 모두 게스트 있음 — 이름이 평소 표본에 없으면 "다른 구성"
        prior_guests = [(r.get("guestname") or "").strip()
                        for r in same_wd if r.get("guestname")]
        if yesterday["guestname"] not in prior_guests:
            differences.append({
                "label": "게스트 구성",
                "yesterday": yesterday["guestname"],
                "common": f"평소 {wd_ko}요일과 다른 게스트 (표본 {len(prior_guests)}건 중 미등장)",
                "impact": "high",
                "category": "guest_different",
            })
            triggers.append("special_guest")

    # daily_corner
    cv, _ = _most_common("daily_corner")
    if yesterday["daily_corner"] and cv and yesterday["daily_corner"] != cv:
        differences.append({
            "label": "일일 코너",
            "yesterday": yesterday["daily_corner"],
            "common": cv,
            "impact": "high",
        })
        triggers.append("corner_change")

    # weekly_corner
    wcv, _ = _most_common("weekly_corner")
    if yesterday["weekly_corner"] and wcv and yesterday["weekly_corner"] != wcv:
        differences.append({
            "label": "주간 코너",
            "yesterday": yesterday["weekly_corner"],
            "common": wcv,
            "impact": "medium",
        })

    # live_yn
    lv, _ = _most_common("live_yn")
    if yesterday["live_yn"] and lv and yesterday["live_yn"] != lv:
        differences.append({
            "label": "방송 형식",
            "yesterday": yesterday["live_yn"],
            "common": lv,
            "impact": "high",
        })
        if "녹음" in yesterday["live_yn"] and "생" in lv:
            triggers.append("live_to_recorded")
        elif "생" in yesterday["live_yn"] and "녹음" in lv:
            triggers.append("recorded_to_live")

    # view_radio_yn
    vv, _ = _most_common("view_radio_yn")
    if yesterday["view_radio_yn"] and vv and yesterday["view_radio_yn"] != vv:
        differences.append({
            "label": "보는라디오",
            "yesterday": ("Y(영상 송출)" if yesterday["view_radio_yn"] == "Y" else "N(미송출)"),
            "common": ("Y(영상 송출)" if vv == "Y" else "N(미송출)"),
            "impact": "high",
        })
        triggers.append("view_radio_toggle")

    # 이벤트(특일) — calendar 온톨로지(공휴일·특일)에 등록된 날짜인지 확인.
    #   라디오 청취는 공휴일/특일에 패턴이 크게 달라져 변화의 직접 원인일 수 있음.
    try:
        from raas_onto import get_adapter
        _dt = get_adapter().get_day_type(row.get("DATE", ""))
        _hol = (_dt or {}).get("holiday_name")
        if _hol:
            differences.append({
                "label": "이벤트(특일)",
                "yesterday": f"{_hol} · {_dt.get('day_type', '')}",
                "common": f"평소 {wd_ko}요일(평일/주말)",
                "impact": "high",
                "category": "calendar_event",
            })
            triggers.append("calendar_event")
    except Exception:
        pass

    if not differences:
        triggers.append("no_change")

    return {
        "ok": True,
        "yesterday": yesterday,
        "differences": differences,
        "triggers": triggers,
        "sample_size": len(same_wd),
        "weekday_ko": _WEEKDAY_KO[target_wd],
    }


def _compute_reference_baseline_check(row: dict, history: Optional[list]) -> dict:
    """1_anchor의 비교 기준일(7일 전 같은 요일) dau가 평소 같은 요일 평균과 얼마나 다른지 점검.

    WoW 변화율의 부분 원인이 "어제가 낮은 게 아니라, 비교 기준이 평소보다 높았던 것"일 수도 있음 (기저 효과).
    특별한 원인을 못 찾았을 때 마무리 안내에 사용.
    """
    from datetime import datetime, timedelta
    target_wd = _date_weekday_int(row.get("DATE", ""))
    today_date = row.get("DATE", "")
    if target_wd is None or not today_date or not history:
        return {"ok": False}
    try:
        dt = datetime.strptime(today_date.replace("-", "/"), "%Y/%m/%d")
    except (ValueError, TypeError):
        return {"ok": False}
    ref_date = (dt - timedelta(days=7)).strftime("%Y/%m/%d")
    ref_date_dash = (dt - timedelta(days=7)).strftime("%Y-%m-%d")

    ref_row = None
    for r in history:
        if r.get("DATE") in (ref_date, ref_date_dash):
            ref_row = r
            break
    if ref_row is None:
        return {"ok": False, "reason": "비교 기준일 행 없음"}
    ref_dau = _safe_float(ref_row.get("dau"))
    if ref_dau is None or ref_dau <= 0:
        return {"ok": False}

    # 평소 같은 요일 평균 (어제·비교 기준일 모두 제외)
    others = []
    for r in history[:-1]:
        d = r.get("DATE", "")
        if d == ref_date or d == ref_date_dash:
            continue
        if _date_weekday_int(d) != target_wd:
            continue
        v = _safe_float(r.get("dau"))
        if v is not None and v > 0:
            others.append(v)
    if len(others) < 2:
        return {"ok": False, "reason": f"비교 표본 {len(others)}건 (≥2 필요)"}

    avg = sum(others) / len(others)
    diff_pct = round((ref_dau - avg) / avg * 100, 1)
    return {
        "ok": True,
        "ref_date": ref_date_dash,
        "ref_dau": int(ref_dau),
        "baseline_avg": int(round(avg)),
        "diff_pct": diff_pct,
        "is_higher_than_usual": diff_pct >= 3.0,
        "is_lower_than_usual": diff_pct <= -3.0,
        "sample_size": len(others),
        "weekday_ko": _WEEKDAY_KO[target_wd],
    }


def _compute_new_user_vs_baseline(row: dict, history: Optional[list]) -> dict:
    """신규 유입 — 어제 vs 평소 같은 요일 평균 (편성 효과 검증)."""
    new_yest = _safe_float(row.get("new"))
    target_wd = _date_weekday_int(row.get("DATE", ""))
    if new_yest is None or target_wd is None or not history:
        return {"ok": False}
    avg, src, n = _weekday_or_simple_avg(
        history[:-1], lambda r: _safe_float(r.get("new")), target_wd
    )
    if avg is None:
        return {"ok": False}
    diff = int(new_yest) - avg
    pct = round(diff / max(abs(avg), 1) * 100)
    return {
        "ok": True,
        "yesterday": int(new_yest),
        "baseline": avg,
        "diff": int(diff),
        "diff_pct": pct,
        "baseline_source": src,
        "sample_size": n,
    }


def _compute_cohort(row: dict) -> dict:
    """전체 + diff(전기 대비 pp) 모두 수집. 유의미 변화는 ≥2pp."""
    out = {}
    for k in ("new_d1_ret", "new_d7_ret", "new_m1_ret",
              "new_d1_ret_pw", "new_d7_ret_pw", "new_m1_ret_pw",
              "new_d1_diff", "new_d7_diff", "new_m1_diff"):
        v = _safe_float(row.get(k))
        if v is not None:
            out[k] = v

    # 평상시와 다른 부분 (전기 대비 절댓값 2pp 이상)
    significant = []
    for level, name in (("d1", "D1"), ("d7", "D7"), ("m1", "M1")):
        ret_v  = out.get(f"new_{level}_ret")
        diff_v = out.get(f"new_{level}_diff")
        if ret_v is None: continue
        if diff_v is not None and abs(diff_v) >= 2.0:
            significant.append({"level": name, "ret": ret_v, "diff": diff_v})
    out["significant_changes"] = significant
    return out


def _compute_stickiness(row: dict, history: Optional[list] = None) -> dict:
    """DAU/MAU + 평상시(같은 요일 평균, 폴백 28일) 대비."""
    dau = _safe_float(row.get("dau"))
    mau = _safe_float(row.get("mau"))
    if dau is None or mau is None or mau == 0:
        return {"ok": False}
    stickiness = round(dau / mau * 100, 1)
    out = {"ok": True, "stickiness_pct": stickiness,
           "dau": int(dau), "mau": int(mau)}

    if history and len(history) > 1:
        target_wd = _date_weekday_int(row.get("DATE", ""))
        def _stick(r):
            d = _safe_float(r.get("dau"))
            m = _safe_float(r.get("mau"))
            if d is None or m is None or m == 0: return None
            return d / m * 100
        avg, src, n = _weekday_or_simple_avg(history[:-1], _stick, target_wd)
        if avg is not None:
            out["baseline_stickiness"] = round(avg, 1)
            out["diff_from_baseline"] = round(stickiness - avg, 1)
            out["baseline_source"] = src
    return out


def _build_cause_raw_text(program_name: str, last_date: str, weekday: str,
                          change_pct: float,
                          flow: dict, cohort: dict, sticky: dict,
                          weekday_check: Optional[dict] = None,
                          programming: Optional[dict] = None,
                          new_user_check: Optional[dict] = None,
                          reference_baseline: Optional[dict] = None,
                          events: Optional[list[dict]] = None,
                          realrate_str: str = "") -> str:
    """온톨로지(TTL) 기반 원시 분석문. 평상시 대비 비교 위주 해석.

    LLM polish가 톤·구조를 다듬어 최종 출력으로 변환.
    (제목 헤더 ### 사용 안 함 — 사용자 요청)
    """
    decomps = _query_decompositions()
    if not decomps:
        return "(원인 분석 온톨로지를 로드할 수 없습니다. rdflib + raas_onto/*.ttl 확인)"

    lines = []
    _rrp = f"({realrate_str})" if realrate_str else ""   # 러브FM: 활성사용자 옆 실청취율
    lines.append(
        f"**{program_name}**의 일간활성사용자{_rrp}가 **전주 동요일 대비 {change_pct:+.1f}%** 변화한 내용을 분석합니다."
    )
    lines.append("")

    # 0) 요일 효과 사전 체크 — 자연스러운 반복 변화인지 먼저
    has_programming_diff = bool(
        programming and programming.get("ok") and programming.get("differences")
    )
    if weekday_check and weekday_check.get("ok"):
        wd = weekday_check
        rng = f"평균 {wd['avg_chg']:+.1f}% · 범위 {wd['min_chg']:+.1f}%~{wd['max_chg']:+.1f}%"
        lines.append("**🗓 요일 효과 체크**")
        if wd["is_within_normal_range"]:
            lines.append(
                f"이번 **{wd['current_chg']:+.1f}%** 변화는 "
                f"평소 {wd['weekday_ko']}요일 변동({rng}) 내로, "
                f"**요일 패턴에 따른 자연스러운 반복 변화**로 보입니다."
            )
            # 자연스러운 범위 안 + 편성 차이 없음 → 기저 효과 안내
            if not has_programming_diff and reference_baseline and reference_baseline.get("ok"):
                rb = reference_baseline
                if rb["is_higher_than_usual"]:
                    lines.append(
                        f"- 참고로 비교 기준일({rb['ref_date']} {rb['weekday_ko']}요일)의 활성사용자({rb['ref_dau']:,}명)는 "
                        f"평소 {rb['weekday_ko']}요일 평균({rb['baseline_avg']:,}명)보다 **{rb['diff_pct']:+.1f}%** 높았던 회차로, "
                        f"이번 감소의 일부는 **기저 효과(평소보다 높았던 회차와의 비교)**로 보입니다."
                    )
                elif rb["is_lower_than_usual"]:
                    lines.append(
                        f"- 참고로 비교 기준일({rb['ref_date']} {rb['weekday_ko']}요일)의 활성사용자({rb['ref_dau']:,}명)는 "
                        f"평소 {rb['weekday_ko']}요일 평균({rb['baseline_avg']:,}명)보다 **{rb['diff_pct']:+.1f}%** 낮았던 회차로, "
                        f"이번 변화율의 일부는 **기저 효과(평소보다 낮았던 회차와의 비교)**로 보입니다."
                    )
        else:
            lines.append(
                f"이번 **{wd['current_chg']:+.1f}%** 변화는 "
                f"평소 {wd['weekday_ko']}요일 변동({rng})을 **벗어났습니다**. "
                f"아래 4가지 축으로 원인을 분해합니다."
            )
        lines.append("")

    bullets = ["①", "②", "③", "④", "⑤", "⑥"]

    for ax in decomps:
        head = f"**{bullets[ax['priority']-1]} {ax['label']}**"
        lines.append(head)

        ax_id = ax["id"]

        if ax_id == "ProgrammingImpactDecomposition":
            # 캘린더 온톨로지 이벤트 우선 노출
            if events:
                for ev in events:
                    lines.append(f"- 🎉 등록된 이벤트: {_format_event_line(ev)}")
            if programming and programming.get("ok"):
                diffs = programming.get("differences") or []
                wd_ko = programming.get("weekday_ko", "")
                sample = programming.get("sample_size", 0)
                if not diffs:
                    lines.append(
                        f"- 어제 편성 구성은 평소 {wd_ko}요일({sample}건 표본)과 동일 — "
                        "편성 외 다른 원인을 살펴봐야 합니다."
                    )
                else:
                    lines.append(f"- 평소 {wd_ko}요일({sample}건 표본)과 다른 편성 요소:")
                    for d in diffs:
                        lines.append(
                            f"  - **{d['label']}**: 어제 \"{d['yesterday']}\" "
                            f"vs 평소 \"{d['common']}\""
                        )
                    # 신규 유입 — 편성 효과 검증
                    if new_user_check and new_user_check.get("ok"):
                        nu = new_user_check
                        sign = "증가" if nu["diff"] > 0 else "감소"
                        if abs(nu["diff_pct"]) >= 10:
                            lines.append("")
                            lines.append(
                                f"- 📊 **신규 유입 영향 검증**: 어제 신규 **+{nu['yesterday']:,}명**, "
                                f"평상시 {wd_ko}요일 평균 +{nu['baseline']:,} 대비 "
                                f"**{abs(nu['diff']):,}명 {sign} ({abs(nu['diff_pct'])}%)** — "
                                f"위 편성 차이의 영향 가능성"
                            )
                        else:
                            lines.append("")
                            lines.append(
                                f"- 신규 유입은 평소 {wd_ko}요일 수준 (+{nu['yesterday']:,}명) — "
                                "편성 차이가 신규 유입엔 큰 영향을 주지 않은 것으로 보입니다."
                            )
            else:
                reason = (programming or {}).get("reason") or "데이터 부족"
                lines.append(f"- ⚠ 편성 영향 분석 불가 — {reason}")

        elif ax_id == "FlowDecomposition":
            # 전주 동요일 대비 레벨 구조분해 (신규/복귀/유지) — 정확히 닫힘
            lines.extend(_flow_decomp_lines(flow.get("wow")))

        elif ax_id == "CohortDecomposition":
            if cohort:
                sig = cohort.get("significant_changes") or []
                d1 = cohort.get("new_d1_ret")
                d7 = cohort.get("new_d7_ret")
                m1 = cohort.get("new_m1_ret")
                if sig:
                    # 평상시와 다른 부분 중점
                    for c in sig:
                        direction = "상승" if c["diff"] > 0 else "하락"
                        lines.append(
                            f"- 신규 {c['level']} 유지율 **{c['ret']:.1f}%** — "
                            f"전기 대비 **{c['diff']:+.1f}pp {direction}** (평상시와 다른 흐름)"
                        )
                    # 평상시와 비슷한 나머지 한 줄로
                    others = []
                    for level, name, v in (("d1", "D1", d1), ("d7", "D7", d7), ("m1", "M1", m1)):
                        if v is None: continue
                        if any(s["level"] == name for s in sig): continue
                        others.append(f"{name} {v:.1f}%")
                    if others:
                        lines.append(f"- 나머지 ({' · '.join(others)})는 평상시와 비슷한 수준")
                else:
                    parts = []
                    for level, name, v in (("d1", "D1", d1), ("d7", "D7", d7), ("m1", "M1", m1)):
                        if v is not None: parts.append(f"{name} {v:.1f}%")
                    if parts:
                        lines.append(f"- 신규 코호트 {' · '.join(parts)} — 평상시와 비슷한 수준")
            else:
                lines.append("- ⚠ 신규 코호트 유지율 데이터 부족")

        elif ax_id == "StickinessDecomposition":
            if sticky.get("ok"):
                pct = sticky["stickiness_pct"]
                bl = sticky.get("baseline_stickiness")
                diff = sticky.get("diff_from_baseline")
                if bl is not None and diff is not None and abs(diff) >= 1.0:
                    direction = "상승" if diff > 0 else "하락"
                    lines.append(
                        f"- DAU/MAU = **{pct:.1f}%** — 평상시({bl:.1f}%) 대비 **{diff:+.1f}pp {direction}**"
                    )
                    if diff > 0:
                        lines.append("- 기존 사용자의 일일 방문 빈도가 평상시보다 높아진 점이 활성사용자 변화에 기여")
                    else:
                        lines.append("- 기존 사용자의 일일 방문 빈도가 평상시보다 낮아진 점이 활성사용자 변화에 기여")
                elif bl is not None:
                    lines.append(f"- DAU/MAU = **{pct:.1f}%** — 평상시({bl:.1f}%)와 비슷한 수준")
                else:
                    lines.append(f"- DAU/MAU = **{pct:.1f}%**")
            else:
                lines.append("- ⚠ DAU 또는 MAU 데이터 부족")

        elif ax_id == "SegmentDecomposition":
            hints = ax.get("ingest_hints", [])
            if not ax["available"] and hints:
                # 세부 적재 안내는 답변 끝 '추가 데이터 고려 사항' 푸터(_data_considerations_block)로 통합.
                lines.append("- 📊 **현재 데이터 부족** — 변화가 어느 사용자 층에서 발생했는지 확인 불가")

        lines.append("")

    return "\n".join(lines)


def _flow_trigger(flow: dict) -> Optional[str]:
    """흐름 분해 결과 → 해석 규칙 트리거 매핑."""
    if not flow.get("ok"):
        return None
    dom = flow.get("dominant_factor")
    val = flow.get("dominant_value", 0)
    if dom == "신규 유입":
        return "new_dominant" if val > 0 else "new_decrease"
    if dom == "복귀":
        return "react_dominant" if val > 0 else "react_decrease"
    if dom == "이탈(추정)":
        return "churn_decrease" if val > 0 else "churn_increase"
    return None


def _cohort_trigger(cohort: dict) -> Optional[str]:
    """코호트 유지율 변화 → 트리거."""
    for k, trig_drop, trig_up in [
        ("new_d1_diff", "d1_drop", "d1_up"),
        ("new_d7_diff", "d7_drop", "d7_up"),
        ("new_m1_diff", "m1_drop", None),
    ]:
        v = cohort.get(k)
        if v is None: continue
        if v < -1.0: return trig_drop
        if v > 1.0 and trig_up: return trig_up
    return None


# ─── KPI 데이터 로더 (Phase 1 ② 즉시 동작 가능) ────────────────────────
def _load_latest_program_kpis(channel: str) -> list[dict]:
    """채널의 최신일 프로그램별 KPI + **전주 동요일 대비 WoW**.

    데이터 소스는 _kpi_rows() 단일 경로 (Splunk timeline provider + CSV fallback).
    WoW가 일간 dau_chg보다 의미 있어 1_anchor TOP 선정 기준으로 사용.

    Returns:
        [{"code", "name", "dau", "dau_prev_wow", "dau_wow_chg"}, ...]
    """
    prefixes = CHANNEL_TO_PROGRAM_PREFIXES.get(channel, [])
    if not prefixes:
        return []

    rows = _kpi_rows()
    if not rows:
        return []

    latest_date = max((r.get("DATE", "") for r in rows if r.get("DATE")), default="")
    if not latest_date:
        return []

    # PGM_CODE → [date 오름차순 행] 인덱스 (WoW 계산용)
    by_code: dict[str, list] = {}
    for r in rows:
        c = r.get("PGM_CODE", "")
        if c:
            by_code.setdefault(c, []).append(r)
    for c in by_code:
        by_code[c].sort(key=lambda r: r.get("DATE", ""))

    programs: list[dict] = []
    for r in rows:
        if r.get("DATE") != latest_date:
            continue
        code = r.get("PGM_CODE", "")
        if not code or len(code) < 3 or code.endswith("00"):
            continue
        if not any(code.startswith(p) for p in prefixes):
            continue

        try:
            dau = int(float(r.get("dau") or 0))
        except (ValueError, TypeError):
            continue
        if dau <= 0:
            continue

        # WoW 계산 (7일 전 같은 요일 행의 dau와 비교)
        wow_chg = _compute_wow_change(by_code.get(code, []), r)
        prev_wow = None
        if wow_chg is not None and dau > 0:
            try:
                prev_wow = round(dau / (1 + wow_chg / 100))
            except (ZeroDivisionError, ValueError):
                prev_wow = None

        programs.append({
            "code": code,
            "name": r.get("PGM_NAME") or code,
            "dau": dau,
            "dau_prev_wow": prev_wow,  # 7일 전 dau
            "dau_wow_chg": wow_chg,    # WoW (전주 동요일 대비) %
            # 주간/월간 (지난주·지난달 봐줘 칩용)
            "wau":      _safe_float(r.get("wau")),
            "wau_prev": _safe_float(r.get("wau_prev")),
            "wau_chg":  _safe_float(r.get("wau_chg")),
            "mau":      _safe_float(r.get("mau")),
            "mau_prev": _safe_float(r.get("mau_prev")),
            "mau_chg":  _safe_float(r.get("mau_chg")),
            "_raw":     r,   # 원본 행 — '가장 두드러진 지표' 태그 계산용
        })
    return programs


# 기간별 '가장 두드러진 지표' 후보: (라벨, %p diff컬럼, polarity)
#   polarity 'good'=값↑이 좋음 / 'bad'=값↑이 나쁨(이탈율). 모두 %(0~100) 단위라 %p 비교 가능.
_NOTABLE_METRICS = {
    "day": [("이탈율", "churn_rate_diff", "bad"), ("복귀율", "react_rate_diff", "good"),
            ("깊은청취율", "deep_rate_diff", "good"), ("실청취율", "real_rate_diff", "good"),
            ("참여율", "engage_rate_diff", "good"), ("습관형성율", "habit_rate_diff", "good"),
            ("D1유지율", "d1_ret_diff", "good"), ("D7유지율", "d7_ret_diff", "good")],
    "week": [("이탈율", "churn_rate_week_diff", "bad"), ("복귀율", "react_rate_week_diff", "good"),
             ("깊은청취율", "deep_rate_week_diff", "good"), ("실청취율", "real_rate_week_diff", "good"),
             ("참여율", "engage_rate_week_diff", "good"), ("습관형성율", "habit_rate_week_diff", "good"),
             ("W1유지율", "w1_ret_diff", "good")],
    "mon": [("이탈율", "churn_rate_mon_diff", "bad"), ("복귀율", "react_rate_mon_diff", "good"),
            ("깊은청취율", "deep_rate_mon_diff", "good"), ("실청취율", "real_rate_mon_diff", "good"),
            ("참여율", "engage_rate_mon_diff", "good"), ("습관형성율", "habit_rate_mon_diff", "good"),
            ("M1유지율", "m1_ret_diff", "good")],
}


def _notable_metric_tags(programs: list[dict], period: str) -> dict:
    """프로그램 code → 두드러진 지표 태그 **리스트**(아이콘+지표+%p).

    각 지표에서 채널 '상승 최고'(📈)·'하락 최고'(📉) 프로그램을 후보로 뽑되,
    아래 2중 게이트를 모두 통과할 때만 태그를 부여한다. 한 프로그램이 여러
    지표에서 통과하면 태그가 누적된다(태그 많음 = 점검 우선순위).

    아이콘은 **실제 변화 부호** 기준 — 채널 max가 양수(+)면 📈(상승 최고),
    min이 음수(−)면 📉(하락 최고). 좋음/나쁨(polarity)이 아니라 방향을 그대로
    보여줘 부호와 항상 일치한다.
      → 모든 프로그램이 감소한 지표: max(가장 덜 떨어짐)는 양수가 아니므로
        📈는 안 붙고, 가장 많이 떨어진 프로그램만 📉로 표시된다.

    게이트:
      ① 절대 효과크기: |diff| >= ABS_FLOOR(%p)
         — 잔잔한 지표의 '복귀율 +0.0%p' 류 무의미 태그 제거.
      ② 상대 이상치:  modified z = 0.6745*|diff-median|/MAD >= Z_MIN
         — 채널 동료 대비 진짜 튀는 값만. median/MAD 사용으로 이상치 1개가
           표준편차를 부풀려 자기 신호를 가리는 masking 회피.
           (MAD=0이면 표준 z = |diff-mean|/sd 로 폴백)
    소표본(n < MIN_N)은 채널 분포를 신뢰할 수 없어 태그하지 않는다.
    """
    import statistics
    ABS_FLOOR = 1.0   # %p — 의미 있는 변화 최소폭
    Z_MIN = 2.0       # 모디파이드 z 임계 (완화 1.5 / 강화 2.5)
    MIN_N = 5         # 상대 이상치 판단 가능한 최소 프로그램 수

    specs = _NOTABLE_METRICS.get(period, [])
    if not specs:
        return {}

    tags: dict = {}
    for label, col, _pol in specs:
        items = []  # (code, diff)
        for p in programs:
            dv = _safe_float((p.get("_raw") or {}).get(col))
            if dv is not None:
                items.append((p["code"], dv))
        if len(items) < MIN_N:
            continue
        vals = [dv for _, dv in items]
        median = statistics.median(vals)
        mad = statistics.median([abs(v - median) for v in vals])
        sd = statistics.pstdev(vals)
        if mad == 0 and sd == 0:
            continue  # 채널 전체 변동 없음

        def _mz(dv):
            if mad > 0:
                return 0.6745 * abs(dv - median) / mad
            return abs(dv - statistics.mean(vals)) / sd  # sd>0 보장

        # 상승 최고(📈) = 채널 max가 실제 양수일 때만 / 하락 최고(📉) = min이 음수일 때만
        max_code, max_dv = max(items, key=lambda x: x[1])
        min_code, min_dv = min(items, key=lambda x: x[1])
        candidates = []  # (code, diff, icon)
        if max_dv > 0:
            candidates.append((max_code, max_dv, "📈"))
        if min_dv < 0 and min_code != max_code:
            candidates.append((min_code, min_dv, "📉"))

        for code, dv, icon in candidates:
            if abs(dv) < ABS_FLOOR:       # ① 절대 게이트
                continue
            if _mz(dv) < Z_MIN:           # ② 상대 게이트
                continue
            tags.setdefault(code, []).append(f"{icon}{label} {dv:+.1f}%p")
    return tags


def _top_changes(programs: list[dict], n: int = 3, mode: str = "pct") -> list[dict]:
    """전주 동요일 대비 TOP N.

    mode:
        'pct' (default) — 변동율(WoW %) 절대값 기준
        'abs'           — 변동수치(WoW 절대값, 명) 기준
    """
    if mode == "abs":
        # 변동수치 = abs(dau - dau_prev_wow)
        candidates = []
        for p in programs:
            cur = p.get("dau")
            prev = p.get("dau_prev_wow")
            if cur is not None and prev is not None:
                candidates.append({**p, "dau_wow_abs": abs(cur - prev)})
        candidates.sort(key=lambda p: p["dau_wow_abs"], reverse=True)
        return candidates[:n]
    # default: pct
    with_chg = [p for p in programs if p.get("dau_wow_chg") is not None]
    with_chg.sort(key=lambda p: abs(p["dau_wow_chg"]), reverse=True)
    return with_chg[:n]


# ─── 슬롯 데이터 컴퓨터 (디스패치) ───────────────────────────────────────
def _compute_pd_program_anchor(user_context: dict) -> tuple[dict, bool]:
    """제작PD 1_anchor: 본인 프로그램의 직전 회차 + 동일 요일 4주 평균 대비.

    KPI CSV만으로 동작 (공감로그/문자는 적재 후 추가될 변수).
    """
    code = user_context.get("program_code")
    if not code:
        return {}, True

    rows = _load_program_history(code, lookback_days=90)
    if not rows:
        return {}, True

    latest = rows[-1]
    last_date = latest.get("DATE", "")
    try:
        curr_dau = int(float(latest.get("dau") or 0))
    except (ValueError, TypeError):
        return {}, True
    if curr_dau <= 0:
        return {}, True

    # 동일 요일 4주 평균 (최신 회차 제외)
    from datetime import datetime
    try:
        latest_dt = datetime.strptime(last_date.replace("-", "/"), "%Y/%m/%d")
        latest_weekday = latest_dt.weekday()
    except (ValueError, TypeError):
        latest_weekday = None

    weekday_daus: list[int] = []
    if latest_weekday is not None:
        for r in rows[:-1]:
            d = r.get("DATE", "")
            try:
                dt = datetime.strptime(d.replace("-", "/"), "%Y/%m/%d")
                if dt.weekday() == latest_weekday:
                    weekday_daus.append(int(float(r.get("dau") or 0)))
            except (ValueError, TypeError):
                continue
    # 가장 가까운 4개만
    recent_weekday_avg = (
        round(sum(weekday_daus[-4:]) / min(4, len(weekday_daus)))
        if weekday_daus else None
    )

    vs_weekday_pct = (
        round((curr_dau - recent_weekday_avg) / recent_weekday_avg * 100, 1)
        if recent_weekday_avg else None
    )

    # 신규 사용자 변화율 (fallback 텍스트의 단서)
    try:
        new_chg = latest.get("new_chg")
        new_user_change_pct = float(new_chg) if new_chg else 0.0
    except (ValueError, TypeError):
        new_user_change_pct = 0.0

    return {
        "program_name": latest.get("PGM_NAME") or code,
        "last_episode_date": last_date.replace("/", "-"),
        "weekday": _weekday_ko(last_date),
        "curr_dau": curr_dau,
        "weekday_avg_dau": recent_weekday_avg or 0,
        "vs_weekday_pct": vs_weekday_pct if vs_weekday_pct is not None else 0.0,
        "new_user_change_pct": new_user_change_pct,
    }, True  # 공감로그·인접 매핑 미적재 → fallback 템플릿이 더 깔끔


def _build_cp_anchor_period(programs: list[dict], channel: str,
                            period: str,
                            sort_mode: str = "pct") -> tuple[dict, bool]:
    """1_anchor — WAU(주간) / MAU(월간) 기준 TOP3 빌더.

    period:
        'weekly'  → wau/wau_prev/wau_chg, '지난주' 헤더
        'monthly' → mau/mau_prev/mau_chg, '지난달' 헤더
    sort_mode:
        'pct' (default) → 변화율 % 절대값 기준
        'abs'           → 변화 명수 (val - prev) 절대값 기준
    """
    if period == "weekly":
        val_f, prev_f, chg_f = "wau", "wau_prev", "wau_chg"
        period_ko = "지난주"
        comparison_ko = "전주 대비"
        metric_label = "WAU"
    else:
        val_f, prev_f, chg_f = "mau", "mau_prev", "mau_chg"
        period_ko = "지난달"
        comparison_ko = "전월 대비"
        metric_label = "MAU"

    # 변화 보유 프로그램만
    have = [p for p in programs if p.get(chg_f) is not None and p.get(val_f) is not None]
    if sort_mode == "abs":
        def _delta(p):
            v = p.get(val_f); pr = p.get(prev_f)
            return (v - pr) if (v is not None and pr is not None) else 0
        have = [p for p in have if p.get(prev_f) is not None]
        have.sort(key=lambda p: abs(_delta(p)), reverse=True)
        basis_label = "변동수치"
    else:
        have.sort(key=lambda p: abs(p[chg_f]), reverse=True)
        basis_label = "변동폭"
    top3 = have[:3]
    if not top3:
        return {}, True

    rows = _kpi_rows()
    last_date_raw = max((r.get("DATE", "") for r in rows if r.get("DATE")), default="")
    last_date = last_date_raw.replace("/", "-")
    weekday = _weekday_ko(last_date_raw)

    top = top3[0]
    top_val = int(top[val_f])
    top_prev = int(top[prev_f]) if top.get(prev_f) is not None else None
    top_chg = top[chg_f]
    top_diff = (top_val - top_prev) if top_prev is not None else None

    # 첫 문장 — sort_mode에 따라 변동수치(명) vs 변동폭(%) 우선
    if sort_mode == "abs" and top_diff is not None:
        raw_sentence = (
            f"{channel}의 {len(programs)}개 프로그램 중 "
            f"{top['name']}이/가 {period_ko}({metric_label}) {comparison_ko} "
            f"{top_diff:+,}명({top_chg:+.1f}%, {top_prev:,}명 → {top_val:,}명)로 변동수치가 가장 컸습니다."
        )
        polish_tone = "- '변동수치가 가장 컸습니다' 톤 유지\n"
    elif top_prev is not None:
        raw_sentence = (
            f"{channel}의 {len(programs)}개 프로그램 중 "
            f"{top['name']}이/가 {period_ko}({metric_label}) {comparison_ko} "
            f"{top_chg:+.1f}% ({top_prev:,}명 → {top_val:,}명)로 변동폭이 가장 컸습니다."
        )
        polish_tone = "- '변동폭이 가장 컸습니다' 톤 유지\n"
    else:
        raw_sentence = (
            f"{channel}의 {len(programs)}개 프로그램 중 "
            f"{top['name']}이/가 {period_ko}({metric_label}) {comparison_ko} "
            f"{top_chg:+.1f}%로 변화가 가장 컸습니다."
        )
        polish_tone = "- '변화가 가장 컸습니다' 톤 유지\n"

    polish_instr = (
        "라디오 데이터 담당자(CP)에게 보고하듯 자연스러운 한국어 한 문장으로 다듬어 주세요.\n"
        "조건:\n"
        "- 한 문장만\n"
        "- 채널명·프로그램명·변화율을 **굵게** 처리 (마크다운)\n"
        "- '이/가' 같은 조사 자동 처리\n"
        f"- '{period_ko}', '{comparison_ko}', '{metric_label}' 표현 반드시 유지\n"
        + polish_tone +
        "- 결과 문장만 출력"
    )
    anchor_sentence = _llm_polish(raw_sentence, instruction=polish_instr, max_tokens=300)

    top_header = f"**TOP 3 ({metric_label} {comparison_ko} {basis_label} 기준)**"
    top_lines = []
    for i, p in enumerate(top3):
        val = int(p[val_f])
        prev = int(p[prev_f]) if p.get(prev_f) is not None else None
        chg = p[chg_f]
        diff = (val - prev) if prev is not None else None
        if prev is not None and diff is not None:
            if i == 0:
                top_lines.append(f"- {p['name']}: {prev:,}명 → {val:,}명 ({diff:+,}명, {chg:+.1f}%)")
            else:
                top_lines.append(f"- {p['name']}: {diff:+,}명 ({chg:+.1f}%)")
        else:
            top_lines.append(f"- {p['name']}: {chg:+.1f}%")
    anchor_answer = anchor_sentence + "\n\n" + top_header + "\n" + "\n".join(top_lines)
    _metric = "WAU" if period == "weekly" else "MAU"
    anchor_answer += (f"\n\n칩을 눌러서 **{top3[0]['name']}**의 {_metric} 변화 원인을 보거나, "
                      "질의창에 다른 보고 싶은 프로그램명을 입력하세요.")
    anchor_answer = _target_block(program=top3[0]["name"], category="규모", metric=metric_label,
                                  period=_PERIOD_LABEL_KO.get(period, "주간"),
                                  sort=_SORT_LABEL_KO.get(sort_mode, "변화율")) + "\n\n" + anchor_answer

    diff_top = (top_val - top_prev) if top_prev is not None else None
    # 동적 chips_next — "왜 변했어?"가 기간에 맞는 원인 슬롯으로 가도록 override
    cause_slot = "2_cause_weekly" if period == "weekly" else "2_cause_monthly"
    chips_next = [
        {
            "label": f"왜 변했어? ({period_ko} 원인 보기)",
            "intent": "explain_change",
            "next_slot": cause_slot,
        },
        {
            "label": "다른 프로그램들도 보기",
            "intent": "show_all_programs",
            "next_slot": "1_anchor_more",
        },
        {
            "label": "최근 개편한 거 효과는?",
            "intent": "show_change_after_revision",
            "next_slot": "4_revision",
        },
    ]
    data = {
        "channel_name": channel,
        "program_count": len(programs),
        "top_change_program": top["name"],
        "top_change_program_code": top["code"],
        "change_pct": top_chg,
        "change_abs": diff_top,
        "prev_dau": top_prev,
        "curr_dau": top_val,
        "last_date": last_date,
        "weekday": weekday,
        "anchor_sentence": anchor_sentence,
        "anchor_answer": anchor_answer,
        "anchor_mode": f"{period}_{sort_mode}",
        "chips_next": chips_next,
    }
    return data, False


def _compute_cp_anchor(user_context: dict,
                       prev_context: Optional[dict] = None,
                       chip_intent: Optional[str] = None) -> tuple[dict, bool]:
    """CP 1_anchor: 채널 프로그램의 변화 TOP 3.

    chip_intent에 따라 분기:
        - 'show_biggest_change_abs' → DAU 변동수치(명) 기준
        - 'show_weekly_top'         → WAU 전주 대비 변화율 기준
        - 'show_monthly_top'        → MAU 전월 대비 변화율 기준
        - 그 외 (default)           → DAU 변동율(%) 기준
    """
    channel = user_context.get("channel_name")
    if not channel:
        return {}, True

    programs = _load_latest_program_kpis(channel)
    if not programs:
        return {}, True

    # WAU/MAU 모드 — 별도 빌더로 위임
    weekly_monthly_map = {
        "show_weekly_top":      ("weekly",  "pct"),
        "show_weekly_top_pct":  ("weekly",  "pct"),
        "show_weekly_top_abs":  ("weekly",  "abs"),
        "show_monthly_top":     ("monthly", "pct"),
        "show_monthly_top_pct": ("monthly", "pct"),
        "show_monthly_top_abs": ("monthly", "abs"),
    }
    if chip_intent in weekly_monthly_map:
        period, sort_mode = weekly_monthly_map[chip_intent]
        return _build_cp_anchor_period(programs, channel, period=period, sort_mode=sort_mode)

    mode = "abs" if chip_intent == "show_biggest_change_abs" else "pct"
    top3 = _top_changes(programs, 3, mode=mode)
    if not top3:
        return {}, True

    top = top3[0]
    rows = _kpi_rows()
    last_date_raw = max((r.get("DATE", "") for r in rows if r.get("DATE")), default="")
    last_date = last_date_raw.replace("/", "-")
    weekday = _weekday_ko(last_date_raw)

    def _abs_diff(p):
        c = p.get("dau"); pr = p.get("dau_prev_wow")
        return (c - pr) if (c is not None and pr is not None) else None

    # mode 분기 — 첫 문장 + TOP3 표현
    if mode == "abs":
        diff = _abs_diff(top) or 0
        raw_sentence = (
            f"{channel}의 {len(programs)}개 프로그램 중 "
            f"{top['name']}이/가 직전 데이터 기준일({last_date} {weekday}요일) "
            f"전주 동요일 대비 {diff:+,}명({top['dau_wow_chg']:+.1f}%, "
            f"{top['dau_prev_wow']:,}명 → {top['dau']:,}명)로 변동수치가 가장 컸습니다."
        )
        top_header = "**TOP 3 (전주 동요일 대비 변동수치 기준)**"
        polish_extra = "- '변동수치가 가장 컸습니다' 톤 유지\n"
    else:
        raw_sentence = (
            f"{channel}의 {len(programs)}개 프로그램 중 "
            f"{top['name']}이/가 직전 데이터 기준일({last_date} {weekday}요일) "
            f"전주 동요일 대비 {top['dau_wow_chg']:+.1f}%"
            f"({top['dau_prev_wow']:,}명 → {top['dau']:,}명)로 변동폭이 가장 컸습니다."
        )
        top_header = "**TOP 3 (전주 동요일 대비 변동폭 기준)**"
        polish_extra = "- '변동폭이 가장 컸습니다' 톤 유지\n"

    polish_instr = (
        "라디오 데이터 담당자(CP)에게 보고하듯 자연스러운 한국어 한 문장으로 다듬어 주세요.\n"
        "조건:\n"
        "- 한 문장만\n"
        "- 채널명·프로그램명·변화율을 **굵게** 처리 (마크다운)\n"
        "- '이/가' 같은 조사 자동 처리\n"
        "- '전주 동요일 대비'라는 표현 반드시 유지\n"
        + polish_extra +
        "- 결과 문장만 출력"
    )
    anchor_sentence = _llm_polish(raw_sentence, instruction=polish_instr, max_tokens=300)

    # TOP 3 줄 (mode별)
    top_lines = []
    for i, p in enumerate(top3):
        diff = _abs_diff(p)
        pct = p.get("dau_wow_chg")
        if i == 0:
            line = (f"- {p['name']}: {p.get('dau_prev_wow', 0):,}명 → {p.get('dau', 0):,}명 "
                    f"({diff:+,}명, {pct:+.1f}%)" if diff is not None else
                    f"- {p['name']}: {pct:+.1f}%")
        else:
            if mode == "abs" and diff is not None:
                line = f"- {p['name']}: {diff:+,}명 ({pct:+.1f}%)"
            else:
                line = f"- {p['name']}: {pct:+.1f}%"
        top_lines.append(line)
    anchor_answer = anchor_sentence + "\n\n" + top_header + "\n" + "\n".join(top_lines)
    anchor_answer += (f"\n\n칩을 눌러서 **{top['name']}**의 DAU 변화 원인을 보거나, "
                      "질의창에 다른 보고 싶은 프로그램명을 입력하세요.")
    anchor_answer = _target_block(program=top["name"], category="규모", metric="DAU",
                                  period="일간",
                                  sort=("변화량" if mode == "abs" else "변화율")) + "\n\n" + anchor_answer

    data = {
        "channel_name": channel,
        "program_count": len(programs),
        "top_change_program": top["name"],
        "top_change_program_code": top["code"],
        "change_pct": top["dau_wow_chg"],
        "change_abs": _abs_diff(top),
        "prev_dau": top["dau_prev_wow"],
        "curr_dau": top["dau"],
        "last_date": last_date,
        "weekday": weekday,
        "anchor_sentence": anchor_sentence,
        "anchor_answer": anchor_answer,
        "anchor_mode": mode,
    }
    return data, False


# ─── 전체 프로그램 스캔 슬롯 (1_anchor_scan) 헬퍼 ─────────────────

# 섹션별 지표 정의 — (라벨, 이번_수치_필드, 이번_수치_단위, 변화_필드, 변화_단위)
_SCAN_METRICS_SCALE = [
    ("DAU",     "dau",     "명", "dau_chg",     "%"),
    ("WAU",     "wau",     "명", "wau_chg",     "%"),
    ("MAU",     "mau",     "명", "mau_chg",     "%"),
    ("롤링WAU", "dau_r7",  "명", "dau_r7_chg",  "%"),
    ("롤링MAU", "dau_r30", "명", "dau_r30_chg", "%"),
]
_SCAN_METRICS_FLOW = [
    ("신규",   "new",        "명", "new_chg",         "%"),
    ("복귀",   "react",      "명", "react_chg",       "%"),
    ("복귀율", "react_rate", "%",  "react_rate_diff", "pp"),
    ("이탈률", "churn_rate", "%",  "churn_rate_diff", "pp"),
]
_SCAN_METRICS_QUALITY = [
    ("실청취율",   "real_rate",   "%", "real_rate_diff",   "pp"),
    ("깊은청취율", "deep_rate",   "%", "deep_rate_diff",   "pp"),
    ("참여율",     "engage_rate", "%", "engage_rate_diff", "pp"),
    ("습관형성률", "habit_rate",  "%", "habit_rate_diff",  "pp"),
]
# 유지율 — (기간라벨, 이번_수치_필드, 변화_필드) — 단위는 모두 %, pp
_SCAN_RETENTION_OVERALL = [
    ("D1", "d1_ret", "d1_ret_diff"),
    ("D7", "d7_ret", "d7_ret_diff"),
    ("W1", "w1_ret", "w1_ret_diff"),
    ("M1", "m1_ret", "m1_ret_diff"),
]
_SCAN_RETENTION_NEW = [
    ("D1", "new_d1_ret", "new_d1_ret_diff"),
    ("D7", "new_d7_ret", "new_d7_ret_diff"),
    ("W1", "new_w1_ret", "new_w1_ret_diff"),
    ("M1", "new_m1_ret", "new_m1_ret_diff"),
]


def _load_scan_program_kpis(channel: str) -> list[dict]:
    """채널의 최신일 프로그램별 전체 KPI 행 + WoW. 1_anchor_scan용."""
    prefixes = CHANNEL_TO_PROGRAM_PREFIXES.get(channel, [])
    if not prefixes:
        return []
    rows = _kpi_rows()
    if not rows:
        return []
    latest_date = max((r.get("DATE", "") for r in rows if r.get("DATE")), default="")
    if not latest_date:
        return []
    # PGM_CODE → 날짜순 행
    by_code: dict[str, list] = {}
    for r in rows:
        c = r.get("PGM_CODE", "")
        if c:
            by_code.setdefault(c, []).append(r)
    for c in by_code:
        by_code[c].sort(key=lambda r: r.get("DATE", ""))

    out: list[dict] = []
    for r in rows:
        if r.get("DATE") != latest_date:
            continue
        code = r.get("PGM_CODE", "")
        if not code or len(code) < 3 or code.endswith("00"):
            continue
        if not any(code.startswith(p) for p in prefixes):
            continue
        dau = _safe_float(r.get("dau")) or 0
        if dau <= 0:
            continue
        wow_chg = _compute_wow_change(by_code.get(code, []), r)
        row_data = dict(r)
        row_data["_code"] = code
        row_data["_name"] = r.get("PGM_NAME") or code
        row_data["_dau"]  = dau
        row_data["_dau_wow_chg"] = wow_chg
        out.append(row_data)
    return out


def _scan_top_per_metric(programs: list[dict], metrics: list[tuple]) -> list[dict]:
    """각 지표마다 |변화|가 가장 큰 프로그램 1개. NULL은 제외.

    Returns: [{label, code, name, current, current_unit, change, change_unit}, ...]
    """
    out = []
    for label, val_field, val_unit, chg_field, chg_unit in metrics:
        best = None
        best_abs = -1.0
        for p in programs:
            v = _safe_float(p.get(chg_field))
            if v is None:
                continue
            av = abs(v)
            if av > best_abs:
                best_abs = av
                best = (p, v)
        if best is None:
            continue
        p, change = best
        out.append({
            "label":        label,
            "code":         p["_code"],
            "name":         p["_name"],
            "current":      _safe_float(p.get(val_field)),
            "current_unit": val_unit,
            "change":       change,
            "change_unit":  chg_unit,
        })
    return out


def _scan_top_retention(programs: list[dict], periods: list[tuple],
                        cohort_label: str) -> Optional[dict]:
    """코호트 유지율 — 각 프로그램에서 |변화|가 가장 큰 기간을 자동 선택 후 전체 TOP 1."""
    best = None
    best_abs = -1.0
    for p in programs:
        max_period = None
        max_change = None
        max_current = None
        max_abs = -1.0
        for period_label, val_field, chg_field in periods:
            v = _safe_float(p.get(chg_field))
            if v is None:
                continue
            av = abs(v)
            if av > max_abs:
                max_abs = av
                max_change = v
                max_current = _safe_float(p.get(val_field))
                max_period = period_label
        if max_period and max_abs > best_abs:
            best_abs = max_abs
            best = {
                "cohort":       cohort_label,
                "period":       max_period,
                "code":         p["_code"],
                "name":         p["_name"],
                "current":      max_current,
                "current_unit": "%",
                "change":       max_change,
                "change_unit":  "pp",
            }
    return best


def _scan_detect_at_risk(programs: list[dict]) -> list[dict]:
    """위험 범위 프로그램 검출. AtRiskProgram_v1 룰 + 신규 정착 실패 + 몰입도 하락."""
    out = []
    for p in programs:
        reasons = []
        dau         = _safe_float(p.get("dau")) or 0
        churn       = _safe_float(p.get("churn_rate"))
        wow         = p.get("_dau_wow_chg")
        new_w1      = _safe_float(p.get("new_w1_ret"))
        new_w1_diff = _safe_float(p.get("new_w1_ret_diff"))
        deep_diff   = _safe_float(p.get("deep_rate_diff"))
        deep_rate   = _safe_float(p.get("deep_rate"))

        # 이탈 위험 (AtRiskProgram_v1)
        if dau >= 1000 and churn is not None and churn >= 30 \
                and wow is not None and wow <= -5:
            reasons.append({
                "kind": "이탈 위험",
                "detail": f"DAU {int(dau):,}명 · 이탈률 {churn:.0f}% · WoW {wow:+.1f}%",
            })
        # 신규 정착 실패
        if new_w1 is not None and new_w1 <= 10 \
                and new_w1_diff is not None and new_w1_diff <= -3:
            reasons.append({
                "kind": "정착 실패",
                "detail": f"신규 W1 유지율 {new_w1:.1f}% (전기 대비 {new_w1_diff:+.1f}pp)",
            })
        # 몰입도 하락
        if dau >= 1000 and deep_diff is not None and deep_diff <= -3:
            label_deep = f"{deep_rate:.0f}%" if deep_rate is not None else "?"
            reasons.append({
                "kind": "몰입도 하락",
                "detail": f"깊은청취율 {label_deep} (전기 대비 {deep_diff:+.1f}pp)",
            })
        if reasons:
            out.append({
                "code": p["_code"],
                "name": p["_name"],
                "reasons": reasons,
            })
    # 위험 사유 수 → DAU 순 정렬
    out.sort(key=lambda x: (-len(x["reasons"]), -float(next(
        (p for p in programs if p["_code"] == x["code"]), {"_dau": 0}
    )["_dau"])))
    return out


def _scan_format_value(value: float, unit: str) -> str:
    """변화량 표시 — 단위·부호 처리."""
    if unit == "%":
        return f"{value:+.1f}%"
    if unit == "pp":
        return f"{value:+.1f}pp"
    return f"{value:+,.0f}"


def _scan_format_current(value: Optional[float], unit: str) -> str:
    """이번 수치 표시 — 단위별 (명/%/pp)."""
    if value is None:
        return "—"
    if unit == "%":
        return f"{value:.1f}%"
    if unit == "pp":
        return f"{value:.1f}pp"
    if unit == "명":
        return f"{int(round(value)):,}명"
    return f"{value:,.0f}"


def _scan_build_chips(rows_program_codes: list[str], channel: str,
                      at_risk_codes: list[str],
                      program_name_by_code: dict[str, str]) -> list[dict]:
    """본문에 등장한 프로그램 빈도순 동적 칩 생성 (최대 5개)."""
    from collections import Counter
    cnt = Counter(rows_program_codes)
    # 빈도 → 등장 순서 보존
    ordered = sorted(cnt.items(), key=lambda kv: -kv[1])
    top = ordered[:5]
    chips = []
    for code, freq in top:
        name = program_name_by_code.get(code, code)
        # 라벨: 위험 진입은 ⚠, 다단면 등장은 🎯
        prefix = "⚠" if code in at_risk_codes else ("🎯" if freq >= 3 else "")
        section_note = f" ({freq}단면)" if freq >= 2 else ""
        label = f"{prefix} {code} {name}{section_note}".strip()
        chips.append({
            "label": label,
            "intent": f"scan_pick_{code}",
            "next_slot": "2_cause",
            "payload": {
                "top_change_program_code": code,
                "top_change_program":      name,
                "channel_name":            channel,
            },
        })
    return chips


# 비-규모 지표 토글 → 그 지표의 전주 대비 변화 큰 순으로 프로그램 정렬 (v2)
#   각: (라벨, 현재값필드, 현재단위, 변화필드, 변화단위)
_ANCHOR_METRIC = {
    "deep":      ("깊은청취율",     "deep_rate",  "%", "deep_rate_diff",  "pp"),
    "retention": ("신규 D7 유지율", "new_d7_ret", "%", "new_d7_ret_diff", "pp"),
    "new_churn": ("신규 유입",      "new",        "명", "new_chg",         "%"),
}
# 신규/이탈은 이탈률을 보조 컬럼으로 함께 표시
_ANCHOR_METRIC_EXTRA = {
    "new_churn": ("이탈률(전주대비)", "churn_rate_diff", "pp"),
}


def _compute_cp_anchor_metric(user_context: dict, prev_context: Optional[dict],
                              metric: str) -> tuple[dict, bool]:
    """CP 1_anchor (v2 비-규모 지표) — 선택 지표의 전주 대비 변화 큰 순으로 정렬.

    규모(scale)는 _compute_cp_anchor(DAU/WAU/MAU)가 담당. 깊은청취/리텐션/신규이탈은
    각 지표 변화 필드로 채널 프로그램을 |변화| 큰 순 TOP5 + 프로그램 칩(→2_cause).
    """
    prev_context = prev_context or {}
    channel = (user_context or {}).get("channel_name") or prev_context.get("channel_name")
    cfg = _ANCHOR_METRIC.get(metric)
    if not channel or not cfg:
        return {}, True
    programs = _load_scan_program_kpis(channel)
    if not programs:
        return {}, True
    label, val_f, val_u, chg_f, chg_u = cfg
    extra = _ANCHOR_METRIC_EXTRA.get(metric)

    ranked = []
    for p in programs:
        c = _safe_float(p.get(chg_f))
        if c is None:
            continue
        ranked.append((abs(c), c, p))
    ranked.sort(key=lambda x: -x[0])
    top = ranked[:5]
    if not top:
        return {"anchor_answer": f"**{channel}** — {label} 변화 데이터를 찾을 수 없습니다."}, True

    last_date = (programs[0].get("DATE") or "").replace("/", "-")
    weekday = _weekday_ko(programs[0].get("DATE", ""))
    head = f"| 프로그램 | {label} | 전주 대비 |" + (f" {extra[0]} |" if extra else "")
    sep = "|---|---|---|" + ("---|" if extra else "")
    lines = [f"**{channel}** — **{label}** 전주 대비 변화 큰 프로그램 ({last_date} {weekday}요일)",
             "", head, sep]
    chips = []
    for _, chg, p in top:
        cur = _scan_format_current(_safe_float(p.get(val_f)), val_u)
        chg_s = _scan_format_value(chg, chg_u)
        ex = ""
        if extra:
            ev = _safe_float(p.get(extra[1]))
            ex = f" {_scan_format_value(ev, extra[2])} |" if ev is not None else " — |"
        lines.append(f"| {p['_name']} | {cur} | {chg_s} |" + ex)
        chips.append({
            "label": f"{p['_code']} {p['_name']}",
            "intent": f"scan_pick_{p['_code']}",
            "next_slot": "2_cause",
            "payload": {
                "top_change_program_code": p["_code"],
                "top_change_program":      p["_name"],
                "channel_name":            channel,
            },
        })
    lines.append("")
    lines.append("어떤 프로그램의 원인을 볼까요?")
    return {"anchor_answer": "\n".join(lines), "chips_next": chips}, False


# ─── 동향 트랙 (v2 1_kpi / 2_impact) — KPI 범주 표 + AU 영향성 분석 ────────
_CHANNEL_CODE = {"파워FM": "F00", "러브FM": "L00", "고릴라M": "G00", "픽채널": "P00"}

# 범주 → (라벨, [(지표라벨, 필드, 종류)])  종류: cnt=명 / rate=%·pp
_KPI_CATEGORIES = {
    "scale":     ("규모",        [("DAU", "dau", "cnt"), ("WAU", "wau", "cnt"), ("MAU", "mau", "cnt"),
                                  ("롤링WAU", "dau_r7", "cnt"), ("롤링MAU", "dau_r30", "cnt")]),
    "flow":      ("사용자 흐름", [("신규", "new", "cnt"), ("복귀", "react", "cnt"),
                                  ("복귀율", "react_rate", "rate"), ("이탈률", "churn_rate", "rate")]),
    "quality":   ("청취 품질",   [("실청취율", "real_rate", "rate"), ("깊은청취율", "deep_rate", "rate"),
                                  ("참여율", "engage_rate", "rate"), ("습관형성률", "habit_rate", "rate")]),
    "retention": ("유지율",      [("D1 유지", "d1_ret", "rate"), ("D7 유지", "d7_ret", "rate"),
                                  ("신규 D1", "new_d1_ret", "rate"), ("신규 D7", "new_d7_ret", "rate")]),
}
_KPI_CATEGORY_PRIMARY = {"scale": "dau_chg", "flow": "new_chg",
                         "quality": "deep_rate_diff", "retention": "new_d7_ret_diff"}
_PERIOD_KO = {"day": "전일", "week": "전주", "month": "전월"}


def _series_date_map(series: list[dict]) -> tuple[dict, list[str]]:
    smap = {}
    for r in series:
        d = (r.get("DATE") or "").replace("/", "-")
        if d:
            smap[d] = r
    return smap, sorted(smap)


def _kpi_deltas(smap: dict, dates: list[str], field: str) -> tuple:
    """(현재, 전일, 전주, 전월) — 전일=직전 가용일, 전주=-7d, 전월=-28d (±3일 근사)."""
    from datetime import datetime, timedelta
    if not dates:
        return (None, None, None, None)
    last = dates[-1]
    cur = _safe_float(smap.get(last, {}).get(field))

    def back(days):
        try:
            tgt = datetime.strptime(last, "%Y-%m-%d") - timedelta(days=days)
        except (ValueError, TypeError):
            return None
        for off in range(0, 4):
            d = (tgt - timedelta(days=off)).strftime("%Y-%m-%d")
            if d in smap:
                return _safe_float(smap[d].get(field))
        return None

    def diff(days):
        v = back(days)
        return (cur - v) if (cur is not None and v is not None) else None

    # 전일은 직전 인덱스 우선
    dod = None
    if len(dates) >= 2:
        pv = _safe_float(smap.get(dates[-2], {}).get(field))
        dod = (cur - pv) if (cur is not None and pv is not None) else None
    return (cur, dod, diff(7), diff(28))


def _kpi_fmt_cur(v, kind: str) -> str:
    if v is None:
        return "—"
    return f"{v:.1f}%" if kind == "rate" else f"{int(round(v)):,}"


def _kpi_fmt_delta(v, kind: str) -> str:
    if v is None:
        return "—"
    return f"{v:+.1f}pp" if kind == "rate" else f"{v:+,.0f}"


# 1_kpi 표 컬럼 — (category, period) → [(지표라벨, 값필드, 변화필드, 종류)]
_KPI_TABLE_COLS = {
    "scale": {
        "day":   [("DAU", "dau", "dau_chg", "cnt"), ("1분↑", "dau_1min", "dau_1min_chg", "cnt"),
                  ("10분↑", "dau_10min", "dau_10min_chg", "cnt"),
                  ("롤링WAU", "dau_r7", "dau_r7_chg", "cnt"), ("롤링MAU", "dau_r30", "dau_r30_chg", "cnt")],
        "week":  [("WAU", "wau", "wau_chg", "cnt"), ("1분↑", "wau_1min", "wau_1min_chg", "cnt"),
                  ("10분↑", "wau_10min", "wau_10min_chg", "cnt")],
        "month": [("MAU", "mau", "mau_chg", "cnt"), ("1분↑", "mau_1min", "mau_1min_chg", "cnt"),
                  ("10분↑", "mau_10min", "mau_10min_chg", "cnt")],
    },
    "flow": {
        "day":   [("신규", "new", "new_chg", "cnt"), ("복귀", "react", "react_chg", "cnt"),
                  ("복귀율", "react_rate", "react_rate_diff", "rate"), ("이탈률", "churn_rate", "churn_rate_diff", "rate")],
        "week":  [("신규", "new_week", "new_week_chg", "cnt"), ("복귀", "react_week", "react_week_chg", "cnt"),
                  ("복귀율", "react_rate_week", "react_rate_week_diff", "rate"), ("이탈률", "churn_rate_week", "churn_rate_week_diff", "rate")],
        "month": [("신규", "new_mon", "new_mon_chg", "cnt"), ("복귀", "react_mon", "react_mon_chg", "cnt"),
                  ("복귀율", "react_rate_mon", "react_rate_mon_diff", "rate"), ("이탈률", "churn_rate_mon", "churn_rate_mon_diff", "rate")],
    },
    "quality": {
        "day":   [("실청취율", "real_rate", "real_rate_diff", "rate"), ("깊은청취율", "deep_rate", "deep_rate_diff", "rate"),
                  ("참여율", "engage_rate", "engage_rate_diff", "rate"), ("습관형성률", "habit_rate", "habit_rate_diff", "rate")],
        "week":  [("실청취율", "real_rate_week", "real_rate_week_diff", "rate"), ("깊은청취율", "deep_rate_week", "deep_rate_week_diff", "rate"),
                  ("참여율", "engage_rate_week", "engage_rate_week_diff", "rate"), ("습관형성률", "habit_rate_week", "habit_rate_week_diff", "rate")],
        "month": [("실청취율", "real_rate_mon", "real_rate_mon_diff", "rate"), ("깊은청취율", "deep_rate_mon", "deep_rate_mon_diff", "rate"),
                  ("참여율", "engage_rate_mon", "engage_rate_mon_diff", "rate"), ("습관형성률", "habit_rate_mon", "habit_rate_mon_diff", "rate")],
    },
    "retention": {
        "day":   [("D1", "d1_ret", "d1_ret_diff", "rate"), ("D7", "d7_ret", "d7_ret_diff", "rate"),
                  ("신규D1", "new_d1_ret", "new_d1_ret_diff", "rate"), ("신규D7", "new_d7_ret", "new_d7_ret_diff", "rate")],
        "week":  [("W1", "w1_ret", "w1_ret_diff", "rate"), ("신규W1", "new_w1_ret", "new_w1_ret_diff", "rate")],
        "month": [("M1", "m1_ret", "m1_ret_diff", "rate"), ("신규M1", "new_m1_ret", "new_m1_ret_diff", "rate")],
    },
}
# 기본 정렬 대표 지표 — (category, period) → (값필드, 변화필드)
_KPI_REP = {
    "scale":     {"day": ("dau", "dau_chg"), "week": ("wau", "wau_chg"), "month": ("mau", "mau_chg")},
    "flow":      {"day": ("new", "new_chg"), "week": ("new_week", "new_week_chg"), "month": ("new_mon", "new_mon_chg")},
    "quality":   {"day": ("deep_rate", "deep_rate_diff"), "week": ("deep_rate_week", "deep_rate_week_diff"),
                  "month": ("deep_rate_mon", "deep_rate_mon_diff")},
    "retention": {"day": ("d1_ret", "d1_ret_diff"), "week": ("w1_ret", "w1_ret_diff"), "month": ("m1_ret", "m1_ret_diff")},
}
_PERIOD_BASELINE = {"day": "전주 동요일 대비", "week": "전주 대비", "month": "전월 대비"}

# 질의창 입력 지표명 → (범주, 지표라벨, 기간힌트). 1_kpi에서 '프로그램+지표' 입력 시 2_impact 라우팅용.
_KPI_METRIC_LOOKUP = [
    (["롤링wau"], "scale", "롤링WAU", "day"),
    (["롤링mau"], "scale", "롤링MAU", "day"),
    (["wau"], "scale", "WAU", "week"),
    (["mau"], "scale", "MAU", "month"),
    (["dau"], "scale", "DAU", "day"),
    (["1분"], "scale", "1분↑", None),
    (["10분"], "scale", "10분↑", None),
    (["깊은청취", "깊은 청취", "deep"], "quality", "깊은청취율", None),
    (["실청취", "real"], "quality", "실청취율", None),
    (["참여"], "quality", "참여율", None),
    (["습관"], "quality", "습관형성률", None),
    (["신규d1", "신규 d1"], "retention", "신규D1", "day"),
    (["신규d7", "신규 d7"], "retention", "신규D7", "day"),
    (["신규w1", "신규 w1"], "retention", "신규W1", "week"),
    (["신규m1", "신규 m1"], "retention", "신규M1", "month"),
    (["w1"], "retention", "W1", "week"),
    (["m1"], "retention", "M1", "month"),
    (["d1"], "retention", "D1", "day"),
    (["d7"], "retention", "D7", "day"),
    (["리텐션", "유지율"], "retention", "D7", "day"),
    (["복귀율"], "flow", "복귀율", None),
    (["복귀"], "flow", "복귀", None),
    (["이탈"], "flow", "이탈률", None),
    (["신규"], "flow", "신규", None),
]


def extract_kpi_metric(text: str):
    """질의창 입력에서 KPI 지표명 추출 → {category, metric, period} 또는 None."""
    t = (text or "").lower()
    for terms, cat, metric, period in _KPI_METRIC_LOOKUP:
        if any(term in t for term in terms):
            return {"category": cat, "metric": metric, "period": period}
    return None


def is_schedule_query(text: str) -> bool:
    """'코너 편성/편성표' 조회 의도 — 원인 분석이 아니라 주간 편성표를 보여줘야 함."""
    t = text or ""
    return ("편성" in t) or ("편성표" in t) or ("코너표" in t)


def build_program_schedule(code: str) -> Optional[str]:
    """프로그램 주간 편성표(요일별 weekly_corner + 매일 고정 daily_corner)
    + 최근 4주 구조적 편성 변경(신설/폐지/일일코너 변경). 마크다운 문자열."""
    from datetime import datetime
    if not code:
        return None
    hist = _load_program_history(code, 35)
    if not hist:
        return None
    latest = hist[-1]
    name = (latest.get("PGM_NAME") or code).strip()
    channel = PROGRAM_PREFIX_TO_CHANNEL.get((code or " ")[0].upper(), "")
    stime = (latest.get("STIME") or "").strip()
    time_s = f"{stime[:2]}:{stime[2:]}" if len(stime) == 4 and stime.isdigit() else stime
    daily = (latest.get("daily_corner") or "").strip()
    last_date = (latest.get("DATE") or "").replace("/", "-")

    # 요일별 최신 weekly_corner (최근 14일 내, 요일별 가장 최근 회차)
    by_wd = {}
    for r in hist[-14:]:
        d = (r.get("DATE") or "").replace("/", "-")
        try:
            wd = datetime.strptime(d, "%Y-%m-%d").weekday()
        except (ValueError, TypeError):
            continue
        if wd not in by_wd or d > by_wd[wd][0]:
            by_wd[wd] = (d, (r.get("weekly_corner") or "").strip())
    wd_ko = "월화수목금토일"

    lines = [f"**{name}** 주간 편성표"]
    meta = " · ".join([x for x in [channel, (time_s if time_s else None),
                                   (f"기준일 {last_date}" if last_date else None)] if x])
    if meta:
        lines.append(meta)
    if daily:
        lines.append(f"\n**매일 고정 코너** — {daily}")
    lines += ["", "**요일별 주간 코너**", "| 요일 | 코너 편성 |", "|---|---|"]
    for wd in range(7):
        if wd in by_wd:
            lines.append(f"| {wd_ko[wd]} | {by_wd[wd][1] or '—'} |")

    # 최근 4주 편성 변경 — 전체 코너 라인업 기준(요일 로테이션은 변경으로 보지 않음)
    recent7, prior3w = hist[-7:], hist[-28:-7]
    latest_set, prior_set = set(), set()
    for r in recent7:
        latest_set |= _corner_tokens(r.get("weekly_corner"))
    for r in prior3w:
        prior_set |= _corner_tokens(r.get("weekly_corner"))
    new_c = latest_set - prior_set
    dropped_c = prior_set - latest_set
    dc = _detect_program_revision(code, window=28).get("daily_change")

    lines += ["", "**최근 4주 편성 변경**"]
    changed = False
    if dc:
        lines.append(f"- 일일코너 변경: **{dc[0]} → {dc[1]}** ({dc[2][5:]}~)")
        changed = True
    if new_c:
        lines.append("- 신설/재개 코너 (이번 주 새로 편성): " + ", ".join(sorted(new_c)[:10]))
        changed = True
    if dropped_c:
        lines.append("- 이번 주 미편성 코너 (직전 3주 방영): " + ", ".join(sorted(dropped_c)[:10]))
        changed = True
    if not changed:
        lines.append("- 최근 4주간 코너 라인업 변경 없음 (요일별 로테이션은 정상 운영)")
    return "\n".join(lines)


def _kpi_cell(p: dict, val_field: str, delta_field: str, kind: str) -> str:
    """셀 = 값 (변화).
    rate: '90.7% (+1.2pp)' / cnt: '38,020 (-2.9% · -1,142)'  — 변화율(소수1자리 %)·변화량(절대).
    """
    v = _safe_float(p.get(val_field))
    if v is None:
        return "—"
    d = _safe_float(p.get(delta_field))
    if kind == "rate":
        return f"{v:.1f}% ({d:+.1f}pp)" if d is not None else f"{v:.1f}%"
    # cnt: delta_field는 변화율(%) — 절대변화량은 value − 전주값(prev)
    val = f"{int(round(v)):,}"
    prev = _safe_float(p.get(val_field + "_prev"))
    parts = []
    if d is not None:
        parts.append(f"{d:+.1f}%")
    if prev is not None:
        parts.append(f"{v - prev:+,.0f}")
    return f"{val} ({' · '.join(parts)})" if parts else val


def _compute_cp_kpi_table(user_context: dict, prev_context: Optional[dict],
                          toggle_state: dict) -> tuple[dict, bool]:
    """1_kpi — 프로그램(행) × 범주·기간 지표(열) 표. 대표지표 변화순 정렬 + 프로그램 칩(→2_impact)."""
    prev_context = prev_context or {}
    channel = (user_context or {}).get("channel_name") or prev_context.get("channel_name")
    category = (toggle_state or {}).get("category", "scale")
    period = (toggle_state or {}).get("period", "day")
    if not channel:
        return {"kpi_table_answer": "담당 채널이 결정되지 않았습니다."}, True
    cols = (_KPI_TABLE_COLS.get(category) or _KPI_TABLE_COLS["scale"]).get(period) \
        or _KPI_TABLE_COLS["scale"]["day"]
    rep_field, rep_delta = (_KPI_REP.get(category) or _KPI_REP["scale"]).get(period, ("dau", "dau_chg"))
    cat_label = (_KPI_CATEGORIES.get(category) or _KPI_CATEGORIES["scale"])[0]

    progs = _load_scan_program_kpis(channel)
    if not progs:
        return {"kpi_table_answer": f"**{channel}** 채널의 프로그램 KPI를 찾을 수 없습니다."}, True
    # 대표 지표 |변화| 큰 순 (값 없으면 뒤로)
    progs.sort(key=lambda p: (_safe_float(p.get(rep_delta)) is None,
                              -abs(_safe_float(p.get(rep_delta)) or 0)))
    last_date = (progs[0].get("DATE") or "").replace("/", "-")
    weekday = _weekday_ko(progs[0].get("DATE", ""))

    head = "| 프로그램 | " + " | ".join(lbl for lbl, vf, df, kind in cols) + " |"
    sep = "|---|" + "---|" * len(cols)
    lines = [f"**{channel} · {cat_label}** 프로그램별 — {last_date} {weekday}요일 · {_PERIOD_BASELINE.get(period, '전주 대비')}",
             "", head, sep]
    _tgt_name = (prev_context or {}).get("top_change_program")  # 특정 프로그램일 때만 (채널 전체면 None)
    for p in progs:
        cells = []
        for lbl, vf, df, kind in cols:
            cells.append(_kpi_cell(p, vf, df, kind))
        nm = f"**{p['_name']}**" if (_tgt_name and p["_name"] == _tgt_name) else p["_name"]
        lines.append(f"| {nm} | " + " | ".join(cells) + " |")
    lines.append("")
    lines.append("표 위 **정렬 기준(변화율·변화량·현재값)**을 고르고 열 머리글을 누르면 그 지표로 정렬됩니다.")
    lines.append("칩을 선택하거나 **프로그램명을 지표명과 함께 입력**하세요.")

    # 칩 = 표에 보이는 지표 항목별로 그 지표 |변화| 1위 프로그램 (열 수만큼).
    #   같은 프로그램이 여러 지표 TOP이어도 라벨의 지표명으로 구별 (코드는 라벨에서 제거, intent/payload엔 유지).
    chips = []
    for lbl, vf, df, kind in cols:
        ranked = [p for p in progs if _safe_float(p.get(df)) is not None]
        if not ranked:
            continue
        top = max(ranked, key=lambda p: abs(_safe_float(p.get(df)) or 0.0))
        chips.append({
            "label": f"{lbl} · {top['_name']}",
            "intent": f"scan_pick_{top['_code']}",
            "next_slot": "2_impact",
            "payload": {"top_change_program_code": top["_code"],
                        "top_change_program": top["_name"],
                        "channel_name": channel, "kpi_metric": lbl, "kpi_aspect": "rate"},
        })
    # 클라이언트가 정렬기준 바뀔 때 지표별 TOP 칩을 재생성하도록 코드 맵 동봉
    meta = {"channel": channel, "codes": {p["_name"]: p["_code"] for p in progs}}
    rep_label = next((lbl for lbl, vf, df, kind in cols if vf == rep_field), None)
    _tgt = (prev_context or {}).get("top_change_program") or f"{channel} 전체"
    _tb = _target_block(program=_tgt, category=cat_label, metric=rep_label,
                        period=_PERIOD_LABEL_KO.get(period, "일간"), sort="변화율")
    return {"kpi_table_answer": _tb + "\n\n" + "\n".join(lines),
            "chips_next": chips, "kpi_meta": meta}, False


_ASPECT_KO = {"rate": "변화율", "abs": "변화량", "value": "현재값"}
# 기간 맞춤 활성사용자 — (값필드, 라벨)
_AU_FIELD = {"day": ("dau", "DAU"), "week": ("wau", "WAU"), "month": ("mau", "MAU")}


def _pearson(xs: list, ys: list) -> Optional[float]:
    """피어슨 상관계수. n<3 또는 분산 0이면 None."""
    n = len(xs)
    if n < 3 or len(ys) != n:
        return None
    mx, my = sum(xs) / n, sum(ys) / n
    cov = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    sx = sum((x - mx) ** 2 for x in xs) ** 0.5
    sy = sum((y - my) ** 2 for y in ys) ** 0.5
    if sx == 0 or sy == 0:
        return None
    return cov / (sx * sy)


def _metric_au_points(series: list, m_field: str, au_field: str,
                      m_kind: str, period: str) -> tuple[list, list]:
    """지표·활성사용자 시계열 포인트 (동일 날짜축). 주/월은 활성사용자 변화 기준 중복 제거."""
    mpts, apts = [], []
    if period == "day":
        for r in series[-28:]:
            d = (r.get("DATE") or "").replace("/", "-")
            mv, av = _safe_float(r.get(m_field)), _safe_float(r.get(au_field))
            if d and mv is not None and av is not None:
                mpts.append({"date": d, "value": round(mv, 1) if m_kind == "rate" else int(round(mv))})
                apts.append({"date": d, "value": int(round(av))})
    else:
        prev = None
        for r in series:
            d = (r.get("DATE") or "").replace("/", "-")
            av, mv = _safe_float(r.get(au_field)), _safe_float(r.get(m_field))
            if not d or av is None or mv is None:
                continue
            iv = int(round(av))
            if iv == prev:
                continue
            prev = iv
            mpts.append({"date": d, "value": round(mv, 1) if m_kind == "rate" else int(round(mv))})
            apts.append({"date": d, "value": iv})
        mpts, apts = mpts[-12:], apts[-12:]
    return mpts, apts


def _impact_status_table(p: dict, cols: list) -> str:
    """지표 현황 — 행=지표, 열=지표·현재·변화율·변화량 (선택 기간 기준)."""
    out = ["| 지표 | 현재 | 변화율 | 변화량 |", "|---|---|---|---|"]
    for lbl, vf, df, kind in cols:
        v, d = _safe_float(p.get(vf)), _safe_float(p.get(df))
        if kind == "rate":
            cur = f"{v:.1f}%" if v is not None else "—"
            rate = f"{d:+.1f}pp" if d is not None else "—"
            amt = "—"
        else:
            cur = f"{int(round(v)):,}" if v is not None else "—"
            rate = f"{d:+.1f}%" if d is not None else "—"
            pv = _safe_float(p.get(vf + "_prev"))
            amt = f"{v - pv:+,.0f}" if (v is not None and pv is not None) else "—"
        out.append(f"| {lbl} | {cur} | {rate} | {amt} |")
    return "\n".join(out)


def _compute_cp_metric_impact(user_context: dict, prev_context: Optional[dict],
                              toggle_state: dict, chip_intent: Optional[str] = None) -> tuple[dict, bool]:
    """2_impact — 선택 프로그램·지표의 영향성 분석.
    헤더(프로그램+범주+지표+기간+정렬기준) / 지표 우선 서술 / 일·주·월 변화 요약 /
    지표↔활성사용자 이중축 그래프+상관 / 같은 범주 주목 변화 / 지표 현황표 / 사용 데이터.
    """
    prev_context = prev_context or {}
    code = prev_context.get("top_change_program_code")
    if not code and chip_intent and chip_intent.startswith("scan_pick_"):
        code = chip_intent[len("scan_pick_"):]
    if not code:
        return {"impact_answer": "분석할 프로그램이 선택되지 않았습니다. 표에서 프로그램을 고르거나 이름을 입력해 주세요."}, True

    category = (toggle_state or {}).get("category", "scale")
    period = (toggle_state or {}).get("period", "day")
    aspect = prev_context.get("kpi_aspect") or "rate"
    cat_label = (_KPI_CATEGORIES.get(category) or _KPI_CATEGORIES["scale"])[0]
    cols = (_KPI_TABLE_COLS.get(category) or _KPI_TABLE_COLS["scale"]).get(period) \
        or _KPI_TABLE_COLS["scale"]["day"]
    # 분석 지표 — 칩이 넘긴 kpi_metric, 없으면 대표 지표
    focus = next((c for c in cols if c[0] == prev_context.get("kpi_metric")), None)
    if focus is None:
        rep_field = (_KPI_REP.get(category) or _KPI_REP["scale"]).get(period, ("dau", "dau_chg"))[0]
        focus = next((c for c in cols if c[1] == rep_field), cols[0])
    flbl, fvf, fdf, fkind = focus

    series = _load_program_history(code, 120)
    if not series:
        return {"impact_answer": f"`{code}` 프로그램 시계열을 찾을 수 없습니다."}, True
    p = series[-1]
    smap, dates = _series_date_map(series)
    name = p.get("PGM_NAME") or prev_context.get("top_change_program") or code
    last = dates[-1]
    weekday = _weekday_ko(last.replace("-", "/"))
    period_ko = _PERIOD_KO.get(period, "전주")
    aspect_ko = _ASPECT_KO.get(aspect, "변화율")
    au_field, au_label = _AU_FIELD.get(period, ("dau", "DAU"))

    # 분석 지표 일/주/월 변화 + 활성사용자
    f_cur, f_dod, f_wow, f_mom = _kpi_deltas(smap, dates, fvf)
    a_cur, a_dod, a_wow, a_mom = _kpi_deltas(smap, dates, au_field)

    # 이중축 그래프 포인트 + 상관계수
    mpts, apts = _metric_au_points(series, fvf, au_field, fkind, period)
    corr = _pearson([x["value"] for x in mpts], [x["value"] for x in apts])
    chart = None
    if len(mpts) >= 2:
        chart = {
            "type": "timeseries_dual",
            "title": f"{name} · {flbl} vs {au_label}",
            "series": [
                {"label": flbl, "unit": "%" if fkind == "rate" else "명", "points": mpts},
                {"label": au_label, "unit": "명", "points": apts},
            ],
            "initial_days": 28 if period == "day" else None,
            "cadence": {"day": "1d", "week": "1w", "month": "1m"}.get(period, "1d"),
            "source": f"raas_kpi_latest.csv:{fvf},{au_field}",
        }

    # 같은 범주 내 가장 눈에 띄는 변화 (분석 지표 제외, 기간 변화 |최대|)
    notable = None
    nkey = -1.0
    for lbl, vf, df, kind in cols:
        if vf == fvf:
            continue
        _, dod, wow, mom = _kpi_deltas(smap, dates, vf)
        chg = {"day": dod, "week": wow, "month": mom}.get(period, wow)
        if chg is not None and abs(chg) > nkey:
            nkey = abs(chg)
            notable = (lbl, chg, kind)

    # ── LLM 서술 (지표 우선 → 일/주/월 → 상관 → 범주 주목) ──
    corr_s = f"{corr:+.2f}" if corr is not None else "산출 불가(데이터 부족)"
    notable_s = (f"{notable[0]} {_kpi_fmt_delta(notable[1], notable[2])}" if notable else "없음")
    raw = (
        f"프로그램 {name} · 범주 {cat_label} · 분석 지표 {flbl} · 기간 {period_ko}\n\n"
        f"[{flbl} 변화]\n"
        f"- 현재 {_kpi_fmt_cur(f_cur, fkind)} / 전일 {_kpi_fmt_delta(f_dod, fkind)} "
        f"/ 전주 {_kpi_fmt_delta(f_wow, fkind)} / 전월 {_kpi_fmt_delta(f_mom, fkind)}\n\n"
        f"[활성사용자 {au_label} (기간 맞춤)]\n"
        f"- 현재 {_kpi_fmt_cur(a_cur, 'cnt')} / 전일 {_kpi_fmt_delta(a_dod, 'cnt')} "
        f"/ 전주 {_kpi_fmt_delta(a_wow, 'cnt')} / 전월 {_kpi_fmt_delta(a_mom, 'cnt')}\n\n"
        f"[{flbl} ↔ {au_label} 상관계수] {corr_s} (최근 {len(mpts)}포인트)\n\n"
        f"[같은 범주 내 가장 큰 변화] {notable_s}\n"
    )
    polish_instr = (
        f"라디오 프로그램 '{name}'의 '{flbl}' 지표 영향성 분석입니다. CP에게 보고하듯 자연스러운 한국어로, 아래 순서로 쓰세요.\n"
        f"1) **'{flbl}' 지표부터** 언급하며 시작 — 이 지표가 어떻게 움직였는지.\n"
        f"2) '{flbl}'의 **일간(전일)·주간(전주)·월간(전월) 변화를 요약**하고, 주목할 변화가 있으면 짚기.\n"
        f"3) '{flbl}'와 **활성사용자({au_label})의 관계** — 위 상관계수·수치 근거로 동행/역행/무관 해석 (아래 이중축 그래프 참조).\n"
        f"4) 끝으로 **같은 범주({cat_label}) 내 가장 눈에 띄는 변화({notable_s})**를 한 줄 언급하며 확인을 유도.\n"
        "조건: 주어진 수치만(추측·날조 금지, '—'=데이터없음), 인과 단정 금지('~경향/~로 보입니다'), "
        "상관계수는 |0.7|↑ 강함·|0.4~0.7| 중간·그 이하 약함으로 해석, '###' 헤더 금지, 결과만 출력."
    )
    polished = _llm_polish(raw, instruction=polish_instr, max_tokens=1100)

    # ── 헤더 블록 + 현황표 + 사용 데이터 (결정적, 비-LLM) ──
    period_label = {"day": "일간", "week": "주간", "month": "월간"}.get(period, "일간")
    header = _target_block(program=name, category=cat_label, metric=flbl,
                           period=period_label, sort=aspect_ko)
    status = "**지표 현황** (현재 · 변화율 · 변화량)\n" + _impact_status_table(p, cols)

    # 사용된 RAAS 실데이터 — 분석에 쓴 모든 필드 + 값
    used, seen = [], set()
    def _add(f):
        if f and f not in seen:
            seen.add(f)
            used.append(f)
    _add(fvf); _add(fdf); _add(fvf + "_prev"); _add(au_field); _add(au_field + "_prev")
    for lbl, vf, df, kind in cols:
        _add(vf); _add(df)
        if kind != "rate":
            _add(vf + "_prev")
    prov_lines = [f"**사용된 RAAS 실데이터** — `raas_kpi_latest.csv` · 기준일 {last} {weekday}요일",
                  f"시계열 {dates[0]}~{last} ({len(dates)}일) · 그래프 {fvf},{au_field} ({len(mpts)}p) · 상관 {corr_s}", ""]
    for f in used:
        val = p.get(f)
        prov_lines.append(f"- `{f}` = {val if val not in (None, '') else '—'}")
    _defs = _metric_definitions_lines(used)
    if _defs:
        prov_lines += ["", "**지표 정의** (온톨로지)"] + _defs
    provenance = "\n".join(prov_lines)

    # 분석대상·사용데이터·추가 데이터 고려 사항은 작은 글씨 참고 블록([small])
    answer = (header + "\n\n"
              + polished + "\n\n" + status
              + "\n\n" + _data_considerations_block("impact")
              + "\n\n[small]\n" + provenance + "\n[/small]")
    out = {"impact_answer": answer}
    if chart:
        out["chart_data"] = chart
    return out, False


# ─── 주간·월간 원인 분석 헬퍼 ──────────────────────────────────

def _collect_recent_guests(code: str, history: list, n_days: int = 7) -> list[dict]:
    """직전 N일치 같은 프로그램의 게스트 출연 기록 모음.

    Returns: [{date, weekday, guests}], 게스트 없는 날 포함 (전체 N일)
    """
    if not history:
        return []
    recent = history[-n_days:]
    out = []
    for r in recent:
        d = (r.get("DATE") or "").replace("/", "-")
        wd = _weekday_ko(r.get("DATE", ""))
        g = (r.get("guestname") or "").strip()
        out.append({"date": d, "weekday": wd, "guests": g})
    return out


def _is_workday(date_str: str) -> bool:
    """평일(주말X·공휴일X) 여부. 어댑터(공휴일 캘린더) 우선, 불가 시 주말만 제외."""
    try:
        from raas_onto import get_adapter
        a = get_adapter()
        if a is not None:
            return a.is_workday(date_str)
    except Exception:
        pass
    from datetime import datetime
    try:
        return datetime.strptime((date_str or "").replace("/", "-"), "%Y-%m-%d").weekday() < 5
    except (ValueError, TypeError):
        return True


def _build_dau_chart_data(program_name: str, history: list, days: int = 28) -> Optional[dict]:
    """일별 DAU 추이 → chart_data (timeseries). 직전 N일.

    각 포인트에 workday(평일 여부) 플래그를 달고 weekday_toggle을 켜, 프론트가
    '평일만' 토글을 즉시(서버 왕복 없이) 적용할 수 있게 한다.
    """
    if not history:
        return None
    recent = history[-days:] if len(history) > days else history
    points = []
    for r in recent:
        d = (r.get("DATE") or "").replace("/", "-")
        v = _safe_float(r.get("dau"))
        if d and v is not None:
            points.append({"date": d, "value": int(v), "workday": _is_workday(d)})
    if len(points) < 2:
        return None
    return {
        "type": "timeseries",
        "chart_type": "line",
        "title": f"{program_name} DAU 추이",
        "points": points,
        "label": "DAU",
        "unit": "",
        "source": "raas_kpi_latest.csv:dau",
        "initial_days": days,
        "cadence": "1d",
        "weekday_toggle": True,
    }


def _build_wau_chart_data(program_name: str, history: list, weeks: int = 12) -> Optional[dict]:
    """주간활성사용자(WAU) 추이 → chart_data (timeseries).

    `wau` 필드는 Sun~Sat 주차의 활성사용자 수로 한 주간 동일값을 유지. 같은 값이
    반복되므로 연속 중복을 제거해 주(週) 단위 1포인트만 남긴다.
    """
    if not history:
        return None
    points = []
    prev_v = None
    for r in history:
        d = (r.get("DATE") or "").replace("/", "-")
        v = _safe_float(r.get("wau"))
        if not d or v is None:
            continue
        iv = int(v)
        if iv == prev_v:
            continue
        points.append({"date": d, "value": iv})
        prev_v = iv
    if len(points) < 2:
        return None
    points = points[-weeks:]
    return {
        "type": "timeseries",
        "chart_type": "line",
        "title": f"{program_name} WAU(주간활성사용자) 추이",
        "points": points,
        "label": "WAU",
        "unit": "",
        "source": "raas_kpi_latest.csv:wau",
        "initial_days": weeks * 7,
        "cadence": "1w",
    }


def _build_mau_chart_data(program_name: str, history: list, months: int = 6) -> Optional[dict]:
    """월간활성사용자(MAU) 추이 → chart_data.

    `mau` 필드는 월 단위 활성사용자 수로 한 달간 동일값을 유지. 연속 중복을 제거해
    월(月) 단위 1포인트만 남긴다.
    """
    if not history:
        return None
    points = []
    prev_v = None
    for r in history:
        d = (r.get("DATE") or "").replace("/", "-")
        v = _safe_float(r.get("mau"))
        if not d or v is None:
            continue
        iv = int(v)
        if iv == prev_v:
            continue
        points.append({"date": d, "value": iv})
        prev_v = iv
    if len(points) < 2:
        return None
    points = points[-months:]
    return {
        "type": "timeseries",
        "chart_type": "line",
        "title": f"{program_name} MAU(월간활성사용자) 추이",
        "points": points,
        "label": "MAU",
        "unit": "",
        "source": "raas_kpi_latest.csv:mau",
        "initial_days": months * 30,
        "cadence": "1mo",
    }


def _build_cause_weekly_text(program_name: str, code: str, row: dict, history: list) -> str:
    """주간 원인 분석 본문 (전주 대비 흐름이 강력한 설명력 발휘)."""
    wau = _safe_float(row.get("wau"))
    wau_chg = _safe_float(row.get("wau_chg"))
    wau_prev = _safe_float(row.get("wau_prev"))
    new_week = _safe_float(row.get("new_week"))
    new_week_chg = _safe_float(row.get("new_week_chg"))
    new_week_prev = _safe_float(row.get("new_week_prev"))
    react_week = _safe_float(row.get("react_week"))
    react_week_chg = _safe_float(row.get("react_week_chg"))
    react_week_prev = _safe_float(row.get("react_week_prev"))
    churn_week = _safe_float(row.get("churn_rate_week"))
    churn_week_diff = _safe_float(row.get("churn_rate_week_diff"))
    churn_week_prev = _safe_float(row.get("churn_rate_week_prev"))
    new_w1 = _safe_float(row.get("new_w1_ret"))
    new_w1_diff = _safe_float(row.get("new_w1_diff"))
    new_m1 = _safe_float(row.get("new_m1_ret"))
    new_m1_diff = _safe_float(row.get("new_m1_diff"))

    lines = []
    # 헤더
    if wau is not None and wau_chg is not None:
        _rr = _lovefm_realrate_str(code, row, "week")
        _rrs = f", {_rr}" if _rr else ""
        lines.append(f"**{program_name}**의 WAU가 **전주 대비 {wau_chg:+.1f}%** 변화했습니다 (WAU **{int(wau):,}명**{_rrs}).")
    else:
        lines.append(f"**{program_name}** 주간 데이터 일부가 부족합니다.")
    lines.append("")

    # ① 편성 영향 (지난주 게스트 요약 — WAU는 직전 주 활성사용자)
    lines.append("**① 편성 영향 분석**")
    guests = _collect_recent_guests(code, history, n_days=7)
    unique_guests: list[str] = []
    if guests:
        seen = set()
        for g in guests:
            raw = (g.get("guests") or "").strip()
            if not raw:
                continue
            for nm in re.split(r"[,/·、]", raw):
                nm = nm.strip()
                if nm and nm not in seen:
                    seen.add(nm)
                    unique_guests.append(nm)
    if unique_guests:
        joined = ", ".join(unique_guests[:8])
        more = f" 외 {len(unique_guests)-8}명" if len(unique_guests) > 8 else ""
        lines.append(f"- 지난주 게스트 출연: **{joined}{more}**")
    else:
        lines.append("- 지난주는 게스트 출연이 없었습니다.")
    # 캘린더 온톨로지 — 지난주(직전 7일) 이벤트
    last_date_w = (row.get("DATE") or "").replace("/", "-")
    if last_date_w:
        from datetime import datetime as _dt, timedelta as _td
        try:
            _to = _dt.strptime(last_date_w, "%Y-%m-%d")
            _from = (_to - _td(days=6)).strftime("%Y-%m-%d")
            week_events = _query_broadcast_events(code, since_date=_from, until_date=last_date_w)
        except Exception:
            week_events = []
    else:
        week_events = []
    if week_events:
        for ev in week_events:
            lines.append(f"- 🎉 이벤트: {_format_event_line(ev)}")
    else:
        lines.append("- 지난주 등록된 특별 이벤트는 없습니다.")
    lines.append("")

    # ② 사용자 흐름 분해 (전주 대비) — 레벨 구조분해(신규/복귀/유지), 정확히 닫힘
    lines.append("**② 사용자 흐름 분해 (전주 대비)**")
    lines.extend(_flow_decomp_lines(_wow_flow_decomp(row, history, "week")))
    lines.append("")

    # ③ 신규 코호트 (주간 W1 + 월간 M1)
    lines.append("**③ 신규 코호트 정착도**")
    has_cohort = False
    if new_w1 is not None:
        line = f"- 신규 W1 유지율: **{new_w1:.1f}%**"
        if new_w1_diff is not None:
            line += f" (전기 대비 {new_w1_diff:+.1f}pp)"
        lines.append(line); has_cohort = True
    if new_m1 is not None:
        line = f"- 신규 M1 유지율: **{new_m1:.1f}%**"
        if new_m1_diff is not None:
            line += f" (전기 대비 {new_m1_diff:+.1f}pp)"
        lines.append(line); has_cohort = True
    if not has_cohort:
        lines.append("- 코호트 데이터 부족")

    return "\n".join(lines)


def _query_broadcast_events(code: str,
                            days: Optional[int] = None,
                            on_date: Optional[str] = None,
                            since_date: Optional[str] = None,
                            until_date: Optional[str] = None) -> list[dict]:
    """캘린더 온톨로지에서 해당 프로그램 관련 이벤트 조회.

    범위 우선순위:
      - on_date 지정 → 정확히 그 날짜
      - since_date / until_date → 범위 (둘 다 inclusive, 둘 중 하나만 지정 가능)
      - days → 오늘 기준 직전 N일
    """
    g = _load_rdf_graph()
    if g is None:
        return []
    q = """
    PREFIX raas: <http://raas.sbs.co.kr/onto#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    SELECT ?label ?date ?anniv ?campaign WHERE {
        ?e a raas:BroadcastEvent ;
            rdfs:label ?label ;
            raas:eventDate ?date ;
            raas:relatedProgram ?prog .
        OPTIONAL { ?e raas:eventAnniversary ?anniv }
        OPTIONAL { ?e raas:eventCampaign ?campaign }
        FILTER(STR(?prog) = "%s")
    } ORDER BY DESC(?date)
    """ % code
    # 범위 결정
    lo: Optional[str] = None
    hi: Optional[str] = None
    if on_date:
        lo = hi = on_date
    elif since_date or until_date:
        lo = since_date
        hi = until_date
    elif days is not None:
        from datetime import datetime, timedelta
        lo = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    out = []
    try:
        for r in g.query(q):
            d = str(r.date)
            if lo and d < lo:
                continue
            if hi and d > hi:
                continue
            out.append({
                "label": str(r.label),
                "date": d,
                "anniversary": int(r.anniv) if r.anniv else None,
                "campaign": str(r.campaign) if r.campaign else None,
            })
    except Exception as e:
        print(f"[ontology] BroadcastEvent query 실패: {e}")
    return out


def _format_event_line(ev: dict) -> str:
    """이벤트 한 줄 포맷: '2026-06-16 **고릴라 데이 (20주년)** (20주년, 노인학대 예방 캠페인 나비새김)'."""
    extras = []
    if ev.get("anniversary"):
        extras.append(f"{ev['anniversary']}주년")
    if ev.get("campaign"):
        extras.append(ev["campaign"])
    tail = f" ({', '.join(extras)})" if extras else ""
    return f"{ev['date']} **{ev['label']}**{tail}"


def _find_notable_guest_days(history: list, n_days: int = 30, spike_pct: float = 15.0) -> list[dict]:
    """직전 N일 중 같은요일 DAU 평균 대비 +spike_pct% 이상이고 게스트가 있던 날을 반환."""
    if not history or len(history) < 14:
        return []
    recent = history[-n_days:] if len(history) > n_days else history[:]
    # 같은요일 베이스라인 산정용: 전체 history에서 요일별 평균
    from collections import defaultdict
    by_wd: dict[int, list[float]] = defaultdict(list)
    for r in history:
        d = (r.get("DATE") or "").replace("/", "-")
        v = _safe_float(r.get("dau"))
        if not d or v is None:
            continue
        try:
            from datetime import date as _date
            y, m, dd = d.split("-")
            wd = _date(int(y), int(m), int(dd)).weekday()
            by_wd[wd].append(v)
        except Exception:
            continue
    notable = []
    for r in recent:
        d = (r.get("DATE") or "").replace("/", "-")
        v = _safe_float(r.get("dau"))
        g_raw = (r.get("guestname") or "").strip()
        if not d or v is None or not g_raw:
            continue
        try:
            from datetime import date as _date
            y, m, dd = d.split("-")
            wd = _date(int(y), int(m), int(dd)).weekday()
        except Exception:
            continue
        # 본인 제외 같은요일 평균
        peers = [x for x in by_wd.get(wd, []) if x != v]
        if len(peers) < 2:
            continue
        baseline = sum(peers) / len(peers)
        if baseline <= 0:
            continue
        pct = (v - baseline) / baseline * 100.0
        if pct >= spike_pct:
            notable.append({
                "date": d,
                "weekday": _weekday_ko(r.get("DATE", "")),
                "guests": g_raw,
                "spike_pct": pct,
            })
    notable.sort(key=lambda x: x["spike_pct"], reverse=True)
    return notable[:5]


def _build_cause_monthly_text(program_name: str, code: str, row: dict, history: list) -> str:
    """월간 원인 분석 본문 (전월 대비 흐름)."""
    mau = _safe_float(row.get("mau"))
    mau_chg = _safe_float(row.get("mau_chg"))
    mau_prev = _safe_float(row.get("mau_prev"))
    new_mon = _safe_float(row.get("new_mon"))
    new_mon_chg = _safe_float(row.get("new_mon_chg"))
    new_mon_prev = _safe_float(row.get("new_mon_prev"))
    react_mon = _safe_float(row.get("react_mon"))
    churn_mon = _safe_float(row.get("churn_rate_mon"))
    new_m1 = _safe_float(row.get("new_m1_ret"))
    new_m1_diff = _safe_float(row.get("new_m1_diff"))

    lines = []
    if mau is not None and mau_chg is not None:
        _rr = _lovefm_realrate_str(code, row, "month")
        _rrs = f", {_rr}" if _rr else ""
        lines.append(f"**{program_name}**의 MAU가 **전월 대비 {mau_chg:+.1f}%** 변화했습니다 (MAU **{int(mau):,}명**{_rrs}).")
    else:
        lines.append(f"**{program_name}** 월간 데이터 일부가 부족합니다.")
    lines.append("")

    # ① 월간 편성 영향 — 게스트 효과 + 이벤트 + 개편
    lines.append("**① 월간 편성 영향**")
    notable = _find_notable_guest_days(history, n_days=30, spike_pct=15.0)
    if notable:
        lines.append("- 게스트 효과가 두드러진 회차:")
        for g in notable:
            lines.append(f"  - {g['date']} ({g['weekday']}) — **{g['guests']}** (DAU {g['spike_pct']:+.0f}%)")
    else:
        lines.append("- 지난달은 사용자 반응이 두드러진 게스트는 없었습니다.")
    events = _query_broadcast_events(code, days=30)
    if events:
        for ev in events:
            lines.append(f"- 🎉 이벤트: {_format_event_line(ev)}")
    else:
        lines.append("- 지난달 등록된 특별 이벤트는 없습니다.")
    lines.append("- 개편 정보: RAAS 미적재 (수기 등록 필요)")
    lines.append("")

    # ② 사용자 흐름 분해 (전월 대비) — 레벨 구조분해(신규/복귀/유지), 정확히 닫힘
    lines.append("**② 사용자 흐름 분해 (전월 대비)**")
    lines.extend(_flow_decomp_lines(_wow_flow_decomp(row, history, "mon")))
    lines.append("")

    # ③ 신규 M1 코호트
    lines.append("**③ 신규 M1 유지율**")
    if new_m1 is not None:
        line = f"- 신규 M1 유지율: **{new_m1:.1f}%**"
        if new_m1_diff is not None:
            line += f" (전기 대비 {new_m1_diff:+.1f}pp)"
        lines.append(line)
    else:
        lines.append("- 데이터 부족")

    return "\n".join(lines)


def _build_period_switch_chart(name: str, code: str, default_period: str = "day") -> Optional[dict]:
    """일/주/월(DAU/WAU/MAU) 추이를 한 차트에 담아 클라이언트가 그래프만 전환하게 함.
    반환 = 기본 기간 차트 + periods(3개 전체) + period_switch 플래그. 본문은 재계산 안 함."""
    hist = _load_program_history(code, 0)
    if not hist:
        return None
    built = {
        "day": _build_dau_chart_data(name, hist),
        "week": _build_wau_chart_data(name, hist),
        "month": _build_mau_chart_data(name, hist),
    }
    periods = {k: v for k, v in built.items() if v}
    if not periods:
        return None
    if default_period not in periods:
        default_period = next(iter(periods))
    base = dict(periods[default_period])
    base["period_switch"] = True
    base["current_period"] = default_period
    base["periods"] = periods
    return base


# ─── 원인 종합(2_cause) 메타 — 분석대상·사용데이터 (작은 글씨 [small]) ─────────
_CAUSE_FIELDS = {
    "day": [
        ("활성사용자", ["dau", "dau_prev", "dau_chg"]),
        ("사용자 흐름", ["new", "new_prev", "new_chg", "react", "react_prev", "churn_rate", "churn_rate_diff"]),
        ("코호트 유지율", ["d1_ret", "d7_ret", "new_d1_ret", "new_d7_ret"]),
        ("청취 품질", ["real_rate", "deep_rate", "dau_1min", "dau_10min"]),
        ("편성", ["guestname", "daily_corner", "weekly_corner"]),
    ],
    "week": [
        ("활성사용자", ["wau", "wau_prev", "wau_chg"]),
        ("사용자 흐름", ["new_week", "react_week", "churn_rate_week"]),
        ("코호트 유지율", ["w1_ret", "new_w1_ret"]),
        ("청취 품질", ["real_rate_week", "deep_rate_week", "wau_1min", "wau_10min"]),
        ("편성", ["weekly_corner"]),
    ],
    "month": [
        ("활성사용자", ["mau", "mau_prev", "mau_chg"]),
        ("사용자 흐름", ["new_mon", "react_mon", "churn_rate_mon"]),
        ("코호트 유지율", ["m1_ret", "new_m1_ret"]),
        ("청취 품질", ["real_rate_mon", "deep_rate_mon", "mau_1min", "mau_10min"]),
    ],
}


_PERIOD_LABEL_KO = {"day": "일간", "week": "주간", "month": "월간", "weekly": "주간", "monthly": "월간"}
_SORT_LABEL_KO = {"pct": "변화율", "abs": "변화량", "rate": "변화율", "value": "현재값"}


def _target_block(program=None, category=None, metric=None, period=None, sort=None) -> str:
    """모든 노드 공통 '분석 대상' 작은 글씨 블록 ([small])."""
    parts = []
    if program:  parts.append(f"프로그램: {program}")
    if category: parts.append(f"범주: {category}")
    if metric:   parts.append(f"지표: {metric}")
    if period:   parts.append(f"기간: {period}")
    if sort:     parts.append(f"정렬기준: {sort}")
    if not parts:
        return ""
    return "[small]\n**분석 대상**\n- " + "  · ".join(parts) + "\n[/small]"


# ─── 추가 데이터 고려 사항 (관리자 참고 [small]) ──────────────────────────
# 더 정확한 분석을 위해 활용/적재를 고려할 데이터.
#   status: "미활용"(룩업에 있으나 분석 미반영) | "미적재"(없음, 적재 필요)
_DATA_CONSIDERATIONS = {
    "cause": [
        {"status": "미활용", "data": "게스트(`guestname`)",
         "why": "게스트 교체·특집이 변화의 직접 원인일 수 있으나 상관 미반영"},
        {"status": "미활용", "data": "코너 편성(`daily_corner`·`weekly_corner`)",
         "why": "코너 신설·폐지가 흐름 변화를 유발(현재 개편 감지에만 사용)"},
        {"status": "미활용", "data": "신규·복귀 비중(`new_share`·`react_share`)",
         "why": "변화가 신규 유입인지 복귀인지 구성비로 명확화"},
        {"status": "미활용", "data": "보이는 라디오·라이브(`view_radio_yn`·`live_yn`)",
         "why": "영상·라이브 회차가 깊은청취·DAU에 영향"},
        {"status": "미적재", "data": "공감로그(회차별 반응)",
         "why": "콘텐츠 반응을 변화에 직접 귀속", "source": "별도 적재"},
        {"status": "미적재", "data": "게스트·이벤트 캘린더(구조화)",
         "why": "특집·이벤트 일자와 변화 시점 정렬", "source": "별도 적재"},
        {"status": "미적재", "data": "성별·연령·시간대 세그먼트",
         "why": "변화가 어느 사용자 층에서 발생했는지 분해", "source": "Splunk 추가 적재"},
    ],
    "impact": [
        {"status": "미활용", "data": "깊은청취 절대 사용자수(`dau_1min`·`dau_10min`)",
         "why": "비율만 보고 규모 변화는 미반영 — 비율·규모 동시 해석 가능"},
        {"status": "미활용", "data": "신규·복귀 비중(`new_share`·`react_share`)",
         "why": "지표 변화의 사용자 구성을 함께 설명"},
        {"status": "미활용", "data": "보이는 라디오·라이브(`view_radio_yn`·`live_yn`)",
         "why": "영상·라이브 여부가 청취품질 지표에 영향"},
        {"status": "미적재", "data": "이벤트·프로모션 캘린더",
         "why": "외부 이벤트가 지표↔활성사용자 상관을 교란하는지 통제", "source": "별도 적재"},
        {"status": "미적재", "data": "세그먼트별 지표",
         "why": "영향이 어느 사용자 층에 집중되는지", "source": "Splunk 추가 적재"},
    ],
    "revision": [
        {"status": "미활용", "data": "게스트(`guestname`) 추이",
         "why": "개편 전후 게스트 구성 변화가 효과에 섞일 수 있음"},
        {"status": "미활용", "data": "보이는 라디오·라이브(`view_radio_yn`·`live_yn`)",
         "why": "개편과 동시에 영상·라이브가 도입됐는지"},
        {"status": "미적재", "data": "개편 메타데이터(일자·내용)",
         "why": "현재 코너 변경으로 개편을 추정 — 정확한 개편 일자·의도 필요", "source": "별도 적재(수기 등록)"},
        {"status": "미적재", "data": "개편 사유·기획 메모",
         "why": "효과 해석의 맥락(왜 바꿨는지)", "source": "별도 적재"},
    ],
}


def _data_considerations_block(analysis_type: str) -> str:
    """'추가 데이터 고려 사항' [small] 블록 — polish 밖에서 결정적으로 조립(LLM 미관여)."""
    items = _DATA_CONSIDERATIONS.get(analysis_type) or []
    if not items:
        return ""
    unused = [it for it in items if it["status"] == "미활용"]
    missing = [it for it in items if it["status"] == "미적재"]
    lines = ["[small]", "**추가 데이터 고려 사항**",
             "더 정확한 분석을 위해 활용·적재를 고려할 데이터입니다.", ""]
    if unused:
        lines.append("**룩업에 있으나 미활용**")
        lines += [f"- {it['data']} — {it['why']}" for it in unused]
        lines.append("")
    if missing:
        lines.append("**미적재 (적재 필요)**")
        for it in missing:
            src = f" · {it['source']}" if it.get("source") else ""
            lines.append(f"- {it['data']}{src} — {it['why']}")
    lines.append("[/small]")
    return "\n".join(lines)


def _lovefm_realrate_str(code: str, row: dict, period: str = "day") -> str:
    """러브FM(L·M 코드)은 앱 첫 진입이 파워FM이라 활성사용자수와 함께 실청취율(1분↑)을 표기.
    활성사용자 옆에 끼워넣을 '실청취율 **X%**' 문자열 반환(타 채널·결측은 '')."""
    if not code or code[0].upper() not in ("L", "M"):
        return ""
    fld = {"week": "real_rate_week", "month": "real_rate_mon"}.get(period, "real_rate")
    try:
        rr = float(row.get(fld))
    except (TypeError, ValueError):
        return ""
    return f"실청취율 **{rr:.1f}%**"


# ─── 1단계(투명성): 스플렁크 필드 → 온톨로지 정의 ──────────────────────────
def _field_meaning(field: str):
    """필드명 → (지표 라벨, 정의) 온톨로지 기반. 없으면 None."""
    try:
        from raas_onto import get_adapter
        info = get_adapter().get_field_info(field)
    except Exception:
        return None
    m = (info or {}).get("metric")
    if not m or not m.get("label"):
        return None
    return (m["label"], (m.get("definition") or "").strip())


def _metric_definitions_lines(fields) -> list:
    """필드 목록 → 사용된 지표의 온톨로지 정의(중복 제거) 줄 목록.
    답변 '참고 정보'에 '이 숫자가 무엇인지'를 명시(스플렁크 데이터 ↔ 온톨로지 의미 연결)."""
    seen = {}
    for f in fields:
        mm = _field_meaning(f)
        if mm and mm[0] not in seen:
            seen[mm[0]] = mm[1]
    return [(f"- {label}: {defn}" if defn else f"- {label}") for label, defn in seen.items()]


# facts.metric 짧은 id → CSV 필드 (온톨로지 정의 조회용)
_QMETRIC_FIELD = {
    "dau": "dau", "wau": "wau", "mau": "mau", "dau_r7": "dau_r7", "dau_r30": "dau_r30",
    "new": "new", "react": "react", "churn": "churn_rate", "real": "real_rate",
    "deep": "deep_rate", "engage": "engage_rate", "habit": "habit_rate",
    "d1": "d1_ret", "d7": "d7_ret", "w1": "w1_ret", "m1": "m1_ret",
    "new_d1": "new_d1_ret", "new_d7": "new_d7_ret", "new_w1": "new_w1_ret", "new_m1": "new_m1_ret",
    "1min": "dau_1min", "10min": "dau_10min",
}
def _query_metric_to_field(mid):
    if not mid or mid == "all":
        return None
    base = mid.replace("_week", "").replace("_mon", "")
    return _QMETRIC_FIELD.get(base, _QMETRIC_FIELD.get(mid, base))


def build_query_provenance(facts, target_date=None) -> str:
    """1단계-c: 일반/자유 질의 답변의 참고 푸터([small]) — 사용 데이터 + 지표 정의(온톨로지).
    관리자에게만 전송(서버), renderAiText가 [small]을 푸터로 렌더 + 참고 토글 연동."""
    facts = facts or {}
    mids = []
    if facts.get("metric"):
        mids.append(facts["metric"])
    for m in (facts.get("metrics") or []):
        if m and m not in mids:
            mids.append(m)
    fields = [_query_metric_to_field(m) for m in mids]
    defs = _metric_definitions_lines([f for f in fields if f])
    src = "데이터: `raas_kpi_latest.csv` / 타임라인"
    if target_date:
        src += f" · 기준일 {target_date}"
    lines = ["[small]", "**사용된 데이터·정의**", src]
    if facts.get("scope"):
        lines.append(f"대상: {facts['scope']}")
    if defs:
        lines += ["", "**지표 정의** (온톨로지)"] + defs
    lines.append("[/small]")
    return "\n".join(lines)


def _ontology_basis_block(code: str = None, date: str = None) -> str:
    """1단계-b: 답변에 '적용한 온톨로지 규칙'을 명시 — 데이터 정의(1a)를 넘어
    어떤 분석 프레임워크·판정 규칙으로 해석했는지. [small] 참고 블록."""
    lines = ["[small]", "**온톨로지 근거** (적용한 분석 규칙)"]
    n0 = len(lines)
    # 변화 분해 프레임워크 (cause 온톨로지)
    try:
        decs = _query_decompositions()
    except Exception:
        decs = []
    if decs:
        lines.append("- 변화 분해 프레임워크 (`raas_ontology_cause.ttl`):")
        for d in decs:
            avail = "" if d.get("available") else " · (데이터 미적재)"
            purpose = (d.get("purpose") or "").strip()
            lines.append(f"  · {d.get('label')} — {purpose}{avail}")
    # 일자 유형 판정 (calendar 온톨로지)
    if date:
        try:
            from raas_onto import get_adapter
            dt = get_adapter().get_day_type(date) or {}
            hol = dt.get("holiday_name")
            jud = f"{hol} ({dt.get('day_type')})" if hol else dt.get("day_type")
            if jud:
                lines.append(f"- 일자 유형 판정 (`raas_ontology_calendar.ttl`): {date} = {jud}")
        except Exception:
            pass
    if len(lines) <= n0:
        return ""
    lines.append("[/small]")
    return "\n".join(lines)


def _cause_meta_blocks(name: str, period: str, prev_context: dict, p: dict) -> tuple[str, str]:
    """원인 종합 분석용 [small] 메타 — (분석대상 헤더, 사용된 RAAS 실데이터) 블록."""
    metric_label = {"day": "DAU", "week": "WAU", "month": "MAU"}.get(period, "DAU")
    am = (prev_context or {}).get("anchor_mode") or ""
    sort_ko = "변화량" if "abs" in am else "변화율"
    header = _target_block(program=name, category="규모", metric=metric_label,
                           period=_PERIOD_LABEL_KO.get(period, "일간"), sort=sort_ko)
    last = (p.get("DATE") or "").replace("/", "-")
    lines = ["[small]", f"**사용된 RAAS 실데이터** — `raas_kpi_latest.csv` · 기준일 {last}", ""]
    _grp_fields = _CAUSE_FIELDS.get(period, _CAUSE_FIELDS["day"])
    for grp, fields in _grp_fields:
        vals = " · ".join(
            f"`{f}`={p.get(f) if p.get(f) not in (None, '') else '—'}" for f in fields)
        lines.append(f"- **{grp}**: {vals}")
    # 지표 정의(온톨로지) — 위 스플렁크 필드가 무엇을 뜻하는지
    _defs = _metric_definitions_lines([f for _, fs in _grp_fields for f in fs])
    if _defs:
        lines += ["", "**지표 정의** (온톨로지)"] + _defs
    lines.append("[/small]")
    provenance = "\n".join(lines)
    # 1단계-b: 온톨로지 근거(적용 규칙) 블록 — provenance 옆에 추가
    _code = (prev_context or {}).get("top_change_program_code")
    _onto = _ontology_basis_block(_code, last)
    if _onto:
        provenance += "\n\n" + _onto
    return header, provenance


def _cause_nav_chips(period: str) -> list:
    """현재 기간을 제외한 다른 두 기간의 '변화 살펴보기' 칩 + 인접 비교 칩."""
    labels = {"day": "일간", "week": "주간", "month": "월간"}
    chips = []
    for p in ("day", "week", "month"):
        if p == period:
            continue
        chips.append({"label": f"📅 {labels[p]} 변화 살펴보기", "intent": f"show_{p}_view",
                      "next_slot": "2_cause", "set_toggle": {"period": p}})
    chips.append({"label": "비슷한 시간대 다른 프로그램은?",
                  "intent": "compare_adjacent_programs", "next_slot": "3_adjacent"})
    chips.append({"label": "다른 지표 변화도 탐색", "intent": "explore_kpi",
                  "next_slot": "1_kpi", "set_toggle": {"category": "flow"}})
    return chips


def _compute_cp_cause_weekly(user_context: dict,
                             prev_context: Optional[dict] = None,
                             chip_intent: Optional[str] = None) -> tuple[dict, bool]:
    """CP 2_cause_weekly: 주간 원인 분석.

    1_anchor에서 결정된 동일 프로그램(top_change_program)에 대해
    WAU 전주 대비 변화를 편성·흐름·코호트로 설명. WAU 추이 그래프 첨부.
    """
    prev_context = prev_context or {}
    code = prev_context.get("top_change_program_code")
    if not code:
        return {"cause_explanation": "분석 대상 프로그램이 결정되지 않았습니다."}, True
    row = _load_program_latest_row(code)
    if not row:
        return {"cause_explanation": f"`{code}` 프로그램 최신 KPI 없음"}, True

    program_name = prev_context.get("top_change_program") or row.get("PGM_NAME") or code
    history = _load_program_history(code, lookback_days=60)

    raw = _build_cause_weekly_text(program_name, code, row, history)
    polish_instr = (
        "다음 주간 원인 분석을 라디오 데이터 담당자(CP)에게 보고하듯 자연스러운 한국어로 다듬어 주세요.\n"
        "- 마크다운 굵게(**...**) · 표 · 줄바꿈 구조 유지\n"
        "- '전주 대비'·'지난주' 같은 시점 표현 반드시 유지 (WAU는 직전 주의 활성사용자이므로 '이번 주'로 바꾸지 말 것)\n"
        "- `[mini]...[/mini]` 마커 그대로 유지\n"
        "- 일반론 권고('점검 필요', '확인 필요') 금지\n"
        "- '###' 헤더 추가 금지\n"
        "- 결과만 출력"
    )
    polished = _llm_polish(raw, instruction=polish_instr, max_tokens=1500)
    chart = _build_period_switch_chart(program_name, code, "week")
    _h, _prov = _cause_meta_blocks(program_name, "week", prev_context, row)
    out = {"cause_explanation": _h + "\n\n" + polished + "\n\n"
           + _data_considerations_block("cause") + "\n\n" + _prov,
           "chips_next": _cause_nav_chips("week")}
    if chart:
        out["chart_data"] = chart
    return out, False


def _compute_cp_cause_monthly(user_context: dict,
                              prev_context: Optional[dict] = None,
                              chip_intent: Optional[str] = None) -> tuple[dict, bool]:
    """CP 2_cause_monthly: 월간 원인 분석 (전월 대비 MAU)."""
    prev_context = prev_context or {}
    code = prev_context.get("top_change_program_code")
    if not code:
        return {"cause_explanation": "분석 대상 프로그램이 결정되지 않았습니다."}, True
    row = _load_program_latest_row(code)
    if not row:
        return {"cause_explanation": f"`{code}` 프로그램 최신 KPI 없음"}, True

    program_name = prev_context.get("top_change_program") or row.get("PGM_NAME") or code
    history = _load_program_history(code, lookback_days=120)

    raw = _build_cause_monthly_text(program_name, code, row, history)
    polish_instr = (
        "다음 월간 원인 분석을 라디오 데이터 담당자(CP)에게 보고하듯 자연스러운 한국어로 다듬어 주세요.\n"
        "- 마크다운 굵게(**...**) · 표 · 줄바꿈 구조 유지\n"
        "- '전월 대비'·'지난달' 같은 시점 표현 반드시 유지 (MAU는 직전 달의 활성사용자이므로 '이번 달'로 바꾸지 말 것)\n"
        "- `[mini]...[/mini]` 마커 그대로 유지\n"
        "- '###' 헤더 추가 금지\n"
        "- 결과만 출력"
    )
    polished = _llm_polish(raw, instruction=polish_instr, max_tokens=1500)
    chart = _build_period_switch_chart(program_name, code, "month")
    _h, _prov = _cause_meta_blocks(program_name, "month", prev_context, row)
    out = {"cause_explanation": _h + "\n\n" + polished + "\n\n"
           + _data_considerations_block("cause") + "\n\n" + _prov,
           "chips_next": _cause_nav_chips("month")}
    if chart:
        out["chart_data"] = chart
    return out, False


def _compute_cp_cause(user_context: dict,
                      prev_context: Optional[dict] = None,
                      chip_intent: Optional[str] = None) -> tuple[dict, bool]:
    """CP 2_cause: 1_anchor의 top program을 4가지 축으로 분해 분석.

    프레임워크: data/ontologies/cause_analysis.json
    - ① 사용자 흐름 분해 (신규/복귀/이탈)
    - ② 코호트 리텐션 (신규 D1/D7/M1)
    - ③ Stickiness (DAU/MAU)
    - ④ 세그먼트 분해 — 현재 RAAS에 미적재 → 부족 데이터 명시

    답변은 온톨로지 기반 raw text 생성 후 Claude로 다듬어 출력.
    """
    prev_context = prev_context or {}
    code = prev_context.get("top_change_program_code")
    if not code:
        return {
            "cause_explanation": (
                "분석 대상 프로그램이 결정되지 않았습니다. 1_anchor 슬롯을 먼저 실행해 주세요."
            )
        }, True

    row = _load_program_latest_row(code)
    if not row:
        return {
            "cause_explanation": f"`{code}` 프로그램의 최신 KPI 행을 찾을 수 없습니다."
        }, True

    # 데이터 계산 — history는 같은 요일 baseline + 패턴 분석용 (직전 90일)
    history = _load_program_history(code, lookback_days=90)
    weekday_check = _compute_weekday_pattern_check(row, history)
    programming = _compute_programming_impact(row, history)
    new_user_check = _compute_new_user_vs_baseline(row, history)
    reference_baseline = _compute_reference_baseline_check(row, history)
    flow = _compute_flow_decomposition(row, history=history)
    # 전주 동요일 대비 레벨 구조분해(신규/복귀/유지) — 정확히 닫히는 메인 분해
    flow["wow"] = _wow_flow_decomp(row, history, "day")
    cohort = _compute_cohort(row)
    sticky = _compute_stickiness(row, history=history)

    program_name = prev_context.get("top_change_program") or row.get("PGM_NAME") or code
    last_date = (row.get("DATE") or "").replace("/", "-")
    weekday = _weekday_ko(row.get("DATE", ""))
    # prev_context의 change_pct(WoW)를 우선, 없으면 직접 계산
    change_pct = (
        _safe_float(prev_context.get("change_pct"))
        or _compute_wow_change(history, row)
        or 0.0
    )

    # 캘린더 온톨로지 — 해당 일자의 이벤트 (예: 고릴라 데이)
    events = _query_broadcast_events(code, on_date=last_date)

    # 원시 분석문 작성 (TTL 온톨로지에서 4축·적재 힌트 SPARQL 조회) → Claude 다듬기
    raw = _build_cause_raw_text(
        program_name=program_name,
        last_date=last_date,
        weekday=weekday,
        change_pct=change_pct,
        flow=flow,
        cohort=cohort,
        sticky=sticky,
        weekday_check=weekday_check,
        programming=programming,
        new_user_check=new_user_check,
        reference_baseline=reference_baseline,
        events=events,
        realrate_str=_lovefm_realrate_str(code, row, "day"),
    )
    polish_instr = (
        "다음 분석 내용을 라디오 데이터 담당자(CP)에게 보고하듯 자연스러운 한국어로 다듬어 주세요.\n\n"
        "조건:\n"
        "- 마크다운 굵게(**...**) · 표 · 줄바꿈 구조 유지\n"
        "- '###'로 시작하는 제목 줄(헤더) 추가 절대 금지\n"
        "- 분석가 톤 (친근하지만 정확)\n"
        "- 중복 표현 제거\n"
        "- 추측은 '~가능성', '~로 보입니다'로 완곡하게\n"
        "- 일반론적 권고('점검 필요', '확인 필요', '검토 필요')는 사용 금지 — 대신 **데이터에서 보이는 평상시 대비 차이**를 구체적으로 강조\n"
        "- '전주 대비'·'전주 동요일 대비'·'직전일 대비' 같은 **시점 표현은 반드시 그대로 유지** (의미 명확성 핵심)\n"
        "- '일간활성사용자' 표현은 그대로 유지 (절대 '활성사용자' 등으로 줄이지 말 것)\n"
        "- 첫 문장의 프로그램명은 그대로 유지\n"
        "- `[mini]...[/mini]` 마커는 **반드시 그대로 유지** (한 줄, 마커 그대로). 안에 텍스트도 절대 변경·확장 금지\n"
        "- 결과만 출력 (서두·해설 없이)"
    )
    polished = _llm_polish(raw, instruction=polish_instr, max_tokens=1500)

    chart = _build_period_switch_chart(program_name, code, "day")  # 일/주/월 그래프 동봉(클라 전환)
    _h, _prov = _cause_meta_blocks(program_name, "day", prev_context, row)
    out = {"cause_explanation": _h + "\n\n" + polished + "\n\n"
           + _data_considerations_block("cause") + "\n\n" + _prov,
           "chips_next": _cause_nav_chips("day")}
    if chart:
        out["chart_data"] = chart
    return out, False


# ─── 데이파트(시간대) 분류 — STIME(HHMM) 기준, 채널별 경계 ────────────────
#  표시 순서: 출퇴근 > 오후 > 오전 > 저녁 > 심야
#  오후(12–17)·오전(09–12)·저녁출퇴근(17–20)은 채널 공통.
#  아침출퇴근 시작 / 저녁 끝 / 심야(자정 넘김)만 채널별 — docs/storyline_cp_redesign_v2.md §6.4
#    파워FM: 아침 07–09, 저녁 20–23, 심야 23–07
#    러브FM: 아침 06–09, 저녁 20–22, 심야 22–06
_DAYPART_ORDER = ["출퇴근", "오후", "오전", "저녁", "심야"]

# 채널별 경계 (morning_start, evening_end=night_start) — HHMM
_CHANNEL_DAYPART = {
    "파워FM": {"morning_start": 700, "night_start": 2300},
    "러브FM": {"morning_start": 600, "night_start": 2200},
}
_DEFAULT_DAYPART = {"morning_start": 600, "night_start": 2400}  # 심야 00–06, 저녁 20–24


def _daypart_of(stime, channel: Optional[str] = None) -> Optional[str]:
    """STIME('HHMM') → 데이파트 라벨 (채널별 경계). 파싱 실패 시 None.

    심야는 night_start ~ 다음날 morning_start로 자정을 넘긴다.
    저녁은 20:00 ~ night_start. 그 외(오후/오전/저녁출퇴근)는 채널 공통.
    """
    try:
        h = int(str(stime).strip())
    except (ValueError, TypeError):
        return None
    if not (0 <= h <= 2359):
        return None
    b = _CHANNEL_DAYPART.get(channel, _DEFAULT_DAYPART)
    ms, ns = b["morning_start"], b["night_start"]
    if h >= ns or h < ms:          # 심야 (자정 넘김)
        return "심야"
    if ms <= h < 900:              # 아침 출퇴근 러시
        return "출퇴근"
    if 900 <= h < 1200:
        return "오전"
    if 1200 <= h < 1700:
        return "오후"
    if 1700 <= h < 2000:           # 저녁 출퇴근 러시 (채널 공통)
        return "출퇴근"
    return "저녁"                   # 20:00 ~ night_start


def _anchor_mode_fields(raw_mode: Optional[str]) -> dict:
    """anchor_mode → 지표 필드/라벨 묶음 (anchor_more·anchor_adjacent 공용)."""
    raw_mode = (raw_mode or "pct").lower()
    if raw_mode == "weekly":  raw_mode = "weekly_pct"
    if raw_mode == "monthly": raw_mode = "monthly_pct"
    if raw_mode in ("weekly_pct", "weekly_abs"):
        return dict(raw_mode=raw_mode, val_f="wau", prev_f="wau_prev", chg_f="wau_chg",
                    metric_label="WAU", compare_ko="전주 대비",
                    sort_pct=(raw_mode == "weekly_pct"), tag_period="week")
    if raw_mode in ("monthly_pct", "monthly_abs"):
        return dict(raw_mode=raw_mode, val_f="mau", prev_f="mau_prev", chg_f="mau_chg",
                    metric_label="MAU", compare_ko="전월 대비",
                    sort_pct=(raw_mode == "monthly_pct"), tag_period="mon")
    return dict(raw_mode=raw_mode, val_f="dau", prev_f="dau_prev_wow", chg_f="dau_wow_chg",
                metric_label="DAU", compare_ko="전주 동요일 대비",
                sort_pct=(raw_mode == "pct"), tag_period="day")


def _anchor_period_label(raw_mode: str, last_date: str) -> str:
    """기준일/지난주/지난달 라벨 (anchor_more·anchor_adjacent 공용)."""
    if not last_date:
        return ""
    from datetime import datetime, timedelta
    try:
        ld = datetime.strptime(last_date, "%Y-%m-%d")
        if raw_mode in ("weekly_pct", "weekly_abs"):
            this_mon = ld - timedelta(days=ld.weekday())
            last_mon = this_mon - timedelta(days=7)
            last_sun = this_mon - timedelta(days=1)
            return f"지난주 ({last_mon.strftime('%Y-%m-%d')} ~ {last_sun.strftime('%Y-%m-%d')})"
        if raw_mode in ("monthly_pct", "monthly_abs"):
            first_this = ld.replace(day=1)
            last_month_last = first_this - timedelta(days=1)
            return f"지난달 ({last_month_last.year}년 {last_month_last.month}월)"
        return f"기준일: {last_date}"
    except Exception:
        return f"기준일: {last_date}"


def _anchor_leaders(rest: list, f: dict) -> tuple:
    """변동폭(변동율) 최고 / 변동수치 최고 프로그램 code (정렬 모드 무관, 절대값 기준)."""
    chg_f, val_f, prev_f = f["chg_f"], f["val_f"], f["prev_f"]
    rate_leader = max(rest, key=lambda p: abs(p[chg_f]))["code"] if rest else None
    def _abs_delta(p):
        v = p.get(val_f); pr = p.get(prev_f)
        return abs(v - pr) if (v is not None and pr is not None) else -1
    cands = [p for p in rest if p.get(prev_f) is not None]
    delta_leader = max(cands, key=_abs_delta)["code"] if cands else None
    return rate_leader, delta_leader


def _anchor_program_line(p: dict, f: dict, notable_tags: dict,
                         rate_leader_code, delta_leader_code,
                         prefix: str) -> Optional[str]:
    """프로그램 1줄 렌더 — 리더 태그(변동폭/변동수치 최고) + 지표 이상치 태그.

    anchor_more(번호)·anchor_adjacent(불릿)가 prefix만 바꿔 공용.
    """
    val = p.get(f["val_f"]); prev = p.get(f["prev_f"]); chg = p.get(f["chg_f"])
    if val is None or chg is None:
        return None
    v_int = int(val)
    lead_tags = []
    if p["code"] == rate_leader_code:
        lead_tags.append("🏅변동폭 최고")
    if p["code"] == delta_leader_code:
        lead_tags.append("🏅변동수치 최고")
    tags_list = lead_tags + (notable_tags.get(p["code"]) or [])
    tag_str = ("  ·  " + "  ".join(tags_list)) if tags_list else ""
    if prev is not None:
        p_int = int(prev)
        diff = v_int - p_int
        return f"{prefix}{p['name']}: {p_int:,}명 → {v_int:,}명 ({diff:+,}명, {chg:+.1f}%){tag_str}"
    return f"{prefix}{p['name']}: {chg:+.1f}%{tag_str}"


def _anchor_icon_legend() -> str:
    """태그 아이콘 범례 ([mini] — 작고 연한 안내 글씨). more·adjacent 공용."""
    return ("[mini]🏅 활성사용자 변동폭·변동수치 최고  ·  "
            "📈/📉 지표별 채널 내 상승/하락 최고 (이상치만 표시)[/mini]")


def _compute_cp_anchor_adjacent(user_context: dict,
                                prev_context: Optional[dict] = None,
                                chip_intent: Optional[str] = None) -> tuple[dict, bool]:
    """CP 3_adjacent — "비슷한 시간대 다른 프로그램은?".

    anchor_more와 동일한 내용(같은 프로그램 줄 + 변동폭/변동수치 최고 + 지표
    이상치 태그)을 보여주되, 나열 방식만 다름:
        시간대(출퇴근/오후/오전/저녁/심야) 그룹 → 그룹 내 STIME(편성) 오름차순.
    지표 모드(DAU/WAU/MAU)는 anchor_mode를 그대로 승계.
    """
    prev_context = prev_context or {}
    channel = (user_context or {}).get("channel_name") or prev_context.get("channel_name")
    if not channel:
        return {"adjacent_answer": "담당 채널이 결정되지 않았습니다."}, True
    programs = _load_latest_program_kpis(channel)
    if not programs:
        return {"adjacent_answer": f"**{channel}** 채널의 프로그램 KPI를 찾을 수 없습니다."}, True

    f = _anchor_mode_fields(prev_context.get("anchor_mode"))
    raw_mode, val_f, chg_f = f["raw_mode"], f["val_f"], f["chg_f"]
    metric_label, compare_ko = f["metric_label"], f["compare_ko"]
    rows = _kpi_rows()
    last_date = max((r.get("DATE", "") for r in rows if r.get("DATE")), default="").replace("/", "-")

    rest = [p for p in programs if p.get(chg_f) is not None and p.get(val_f) is not None]
    if not rest:
        return {"adjacent_answer": f"**{channel}**에서 표시할 프로그램이 없습니다."}, False

    notable_tags = _notable_metric_tags(programs, f["tag_period"])
    rate_leader_code, delta_leader_code = _anchor_leaders(rest, f)

    # 시간대 그룹핑 (표시 순서는 _DAYPART_ORDER) + 그룹 내 편성(STIME) 오름차순
    groups: dict = {label: [] for label in _DAYPART_ORDER}
    unknown: list = []
    for p in rest:
        dp = _daypart_of((p.get("_raw") or {}).get("STIME"), channel)
        (groups[dp] if dp else unknown).append(p)

    def _stime_key(p, night=False):
        try:
            h = int(str((p.get("_raw") or {}).get("STIME")).strip())
        except (ValueError, TypeError):
            return 9999
        # 심야는 자정을 넘김 → 23시대를 자정 이후(01·03·05시)보다 앞으로
        if night and h >= 1200:
            h -= 2400
        return h
    for label in groups:
        groups[label].sort(key=lambda p: _stime_key(p, night=(label == "심야")))
    unknown.sort(key=_stime_key)

    # "왜 그랬어?"에서 분석한 프로그램이 속한 시간대 → 섹션 헤더 강조
    analyzed_code = prev_context.get("top_change_program_code")
    analyzed_name = prev_context.get("top_change_program")
    analyzed_dp = None
    if analyzed_code:
        arow = next((p for p in programs if p["code"] == analyzed_code), None)
        if arow:
            analyzed_dp = _daypart_of((arow.get("_raw") or {}).get("STIME"), channel)

    lines: list[str] = []
    lines.append(f"**{channel}** 시간대별 프로그램 — **{metric_label} {compare_ko}** 변화 (편성 순)")
    period_label_ko = _anchor_period_label(raw_mode, last_date)
    if period_label_ko:
        lines.append(f"({period_label_ko})")
    lines.append(_anchor_icon_legend())

    for label in _DAYPART_ORDER:
        members = groups[label]
        if not members:
            continue
        lines.append("")
        if label == analyzed_dp:
            # [hl] 마커 → 프론트에서 강조색 렌더 (escapeHtml 통과 후 변환)
            note = f" ← {analyzed_name} 시간대" if analyzed_name else " ← 분석한 프로그램 시간대"
            lines.append(f"[hl]🕘 {label}{note}[/hl]")
        else:
            lines.append(f"**🕘 {label}**")
        for p in members:
            line = _anchor_program_line(p, f, notable_tags,
                                        rate_leader_code, delta_leader_code, prefix="- ")
            if line:
                lines.append(line)
    if unknown:
        lines.append("")
        lines.append("**🕘 편성시간 미상**")
        for p in unknown:
            line = _anchor_program_line(p, f, notable_tags,
                                        rate_leader_code, delta_leader_code, prefix="- ")
            if line:
                lines.append(line)

    lines.append("")
    lines.append("원인을 살펴볼 **프로그램명을 입력**해 주세요.")

    _ap = "weekly" if raw_mode.startswith("weekly") else ("monthly" if raw_mode.startswith("monthly") else "day")
    _tgt = prev_context.get("top_change_program") or f"{channel} 전체"
    _tb = _target_block(program=_tgt, category="규모", metric=metric_label,
                        period=_PERIOD_LABEL_KO.get(_ap, "일간"),
                        sort=("변화량" if "abs" in raw_mode else "변화율"))
    data = {
        "adjacent_answer": _tb + "\n\n" + "\n".join(lines),
        "channel_name": channel,
        "anchor_mode": raw_mode,
    }
    return data, False


_CORNER_WINDOW = 35  # 최근 5주


def _corner_tokens(s: str) -> set:
    """코너 문자열 → 개별 코너 토큰 set ('(1부)'·HTML엔티티·공백 정리)."""
    import re
    s = (s or "").replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">").replace("&apos;", "'")
    out = set()
    for part in s.split(","):
        t = re.sub(r"\([^)]*\)", "", part)        # (1부)·(2~3부) 등 제거
        t = re.sub(r"\s+", " ", t).strip(" .·")
        if t:
            out.add(t)
    return out


def _corner_fmt(items: set, cap: int = 3) -> str:
    items = sorted(items)
    return ", ".join(items) if len(items) <= cap else ", ".join(items[:cap]) + f" 외 {len(items)-cap}개"


def _detect_program_revision(code: str, window: int = _CORNER_WINDOW,
                             effect_half_days: int = 14) -> dict:
    """프로그램 1개의 최근 N일 코너 개편 감지 + 사후 DAU 효과 (1·2단계 공용).

    Args:
        window: 코너 개편을 탐지하는 범위(일). 기본 35일(5주).
        effect_half_days: 개편 전후 DAU 평균을 낼 한쪽 창(일). 비교창 토글(2/4/8주) 연동.

    반환: {daily_change, new_c, dropped_c, signal, effect}
      daily_change: (이전, 새, 변경일) | None
      effect: (전평균, 후평균, %, 전일수, 후일수, 기준일) | ('short', 기준일, 후일수) | None
    """
    from collections import defaultdict
    from datetime import datetime

    # DAU 전후 평균은 탐지 범위보다 넓은 풀이 필요 — 큰 풀을 따로 적재
    hist_all = _load_program_history(code, window + effect_half_days + 7)
    rows = hist_all[-window:]   # 코너 개편 탐지는 기존 범위(window) 유지
    if not rows:
        return {"daily_change": None, "new_c": set(), "dropped_c": set(), "signal": 0, "effect": None}

    # 일일코너 전환 — 연속 중복 제거 후 마지막 전환
    daily_seq = []
    for r in rows:
        dc = (r.get("daily_corner") or "").strip()
        if dc and (not daily_seq or daily_seq[-1][1] != dc):
            daily_seq.append((r.get("DATE", "").replace("/", "-"), dc))
    daily_change = (daily_seq[-2][1], daily_seq[-1][1], daily_seq[-1][0]) if len(daily_seq) >= 2 else None

    # 주간코너 — 같은 요일끼리 주차 비교
    by_wd = defaultdict(list)
    for r in rows:
        d = r.get("DATE", "")
        try:
            wd = datetime.strptime(d.replace("/", "-"), "%Y-%m-%d").weekday()
        except (ValueError, TypeError):
            continue
        by_wd[wd].append((d, _corner_tokens(r.get("weekly_corner"))))
    new_c, dropped_c = set(), set()
    for seq in by_wd.values():
        if len(seq) < 2:
            continue
        seq.sort(key=lambda x: x[0])
        latest = seq[-1][1]
        prior = set().union(*(s[1] for s in seq[:-1]))
        new_c |= (latest - prior)
        dropped_c |= (prior - latest)
    moved = new_c & dropped_c          # 요일만 옮긴 로테이션 → 개편 아님
    new_c -= moved
    dropped_c -= moved

    signal = (2 if daily_change else 0) + (1 if new_c else 0) + (1 if dropped_c else 0)

    # 개편 기준일(anchor) + 사후 DAU 효과
    anchor = None
    if daily_change:
        anchor = daily_change[2]
    elif new_c:
        for r in rows:
            if _corner_tokens(r.get("weekly_corner")) & new_c:
                anchor = r.get("DATE", "").replace("/", "-"); break
    elif dropped_c:
        for r in rows:
            if _corner_tokens(r.get("weekly_corner")) & dropped_c:
                anchor = r.get("DATE", "").replace("/", "-")
    effect = None
    if anchor:
        dau_by_date = {}
        for r in hist_all:   # 넓은 풀에서 전후 평균 — 비교창(effect_half_days)만큼 확보
            d = r.get("DATE", "").replace("/", "-")
            v = _safe_float(r.get("dau"))
            if d and v is not None:
                dau_by_date.setdefault(d, v)
        items = sorted(dau_by_date.items())
        before = [v for d, v in items if d < anchor][-effect_half_days:]
        after = [v for d, v in items if d >= anchor][:effect_half_days]
        # 양쪽 1주 이상일 때만 산정 — 요일 구성 균형(주말 편향 제거)
        if len(before) >= 7 and len(after) >= 7:
            b = sum(before) / len(before)
            a = sum(after) / len(after)
            pct = round((a - b) / b * 100, 1) if b else None
            effect = (round(b), round(a), pct, len(before), len(after), anchor)
        else:
            effect = ("short", anchor, len(after))

    return {"daily_change": daily_change, "new_c": new_c, "dropped_c": dropped_c,
            "signal": signal, "effect": effect}


def _revision_summary_line(rev: dict) -> str:
    """개편 내용 한 줄 요약 (1단계 목록용)."""
    parts = []
    if rev["daily_change"]:
        ov, nv, dt = rev["daily_change"]
        parts.append(f"일일코너 {ov} → {nv} ({dt[5:]}~)")
    if rev["new_c"]:
        parts.append(f"신설 {_corner_fmt(rev['new_c'])}")
    if rev["dropped_c"]:
        parts.append(f"폐지 {_corner_fmt(rev['dropped_c'])}")
    return " · ".join(parts) if parts else "변동 없음"


def _compute_cp_revision_effect(user_context: dict,
                                prev_context: Optional[dict] = None,
                                chip_intent: Optional[str] = None) -> tuple[dict, bool]:
    """CP 4_revision (1단계) — 채널에서 개편 감지된 프로그램 목록 + 프로그램별 칩.

    프로그램명 칩(개편 감지 수만큼)을 누르면 2단계(4_revision_detail)에서
    해당 프로그램의 사후 효과를 분석한다.
    """
    prev_context = prev_context or {}
    channel = (user_context or {}).get("channel_name") or prev_context.get("channel_name")
    if not channel:
        return {"revision_answer": "담당 채널이 결정되지 않았습니다."}, True
    programs = _load_latest_program_kpis(channel)
    if not programs:
        return {"revision_answer": f"**{channel}** 채널의 프로그램 KPI를 찾을 수 없습니다."}, True

    rows_all = _kpi_rows()
    last_date = max((r.get("DATE", "") for r in rows_all if r.get("DATE")), default="").replace("/", "-")

    detected = []
    for p in programs:
        rev = _detect_program_revision(p["code"])
        if rev["signal"] > 0:
            detected.append((p, rev))
    detected.sort(key=lambda x: x[1]["signal"], reverse=True)

    lines = [_target_block(program=f"{channel} 전체", category="개편"), "",
             f"**{channel}** 최근 5주 개편 감지 — **{len(detected)}개** (전체 {len(programs)}개 프로그램)"]
    if last_date:
        lines.append(f"(기준일: {last_date} · 최근 35일, 같은 요일끼리 주차 비교)")
    lines.append("")
    if not detected:
        lines.append("최근 5주간 코너 개편이 감지된 프로그램이 없습니다.")
        return {"revision_answer": "\n".join(lines), "channel_name": channel}, False

    for p, rev in detected:
        lines.append(f"- **{p['name']}** — {_revision_summary_line(rev)}")
    lines.append("")
    lines.append("아래에서 프로그램을 선택하면 **개편 사후 효과**를 분석해 드립니다.")
    lines.append("")
    lines.append("※ 주간코너는 게스트·특집 로테이션이 섞일 수 있어, 확실한 개편 신호는 일일코너 변경입니다.")

    chips = [{"label": p["name"], "intent": f"revision_pick_{p['code']}",
              "next_slot": "4_revision_detail"} for p, _ in detected]
    return {"revision_answer": "\n".join(lines), "channel_name": channel,
            "chips_next": chips}, False


def _compute_cp_revision_detail(user_context: dict,
                                prev_context: Optional[dict] = None,
                                chip_intent: Optional[str] = None) -> tuple[dict, bool]:
    """CP 4_revision_detail (2단계) — 선택 프로그램의 개편 내용 + 사후 DAU 효과 + 추이 차트."""
    prev_context = prev_context or {}
    code = None
    if chip_intent and chip_intent.startswith("revision_pick_"):
        code = chip_intent[len("revision_pick_"):]
    code = code or prev_context.get("revision_code")
    if not code:
        return {"revision_detail_answer": "분석할 프로그램이 선택되지 않았습니다. 개편 목록에서 프로그램을 선택해 주세요."}, True

    # 비교창 토글(2/4/8주) → 개편 전후 DAU 평균 창(한쪽 일수)
    _win_map = {"2w": 14, "4w": 28, "8w": 56}
    eff_half = _win_map.get((prev_context or {}).get("window"), 14)
    win_weeks = eff_half // 7

    hist = _load_program_history(code, 60)
    name = (hist[-1].get("PGM_NAME") if hist else None) or code
    rev = _detect_program_revision(code, effect_half_days=eff_half)

    rows_all = _kpi_rows()
    last_date = max((r.get("DATE", "") for r in rows_all if r.get("DATE")), default="").replace("/", "-")

    lines = [_target_block(program=name, category="개편", period=f"전후 {win_weeks}주 비교"), "",
             f"**{name}** 개편 사후 효과"]
    if last_date:
        lines.append(f"(기준일: {last_date} · 최근 35일 · 전후 {win_weeks}주 비교)")
    lines.append("")
    lines.append("**① 개편 내용**")
    dc = rev["daily_change"]
    lines.append("- 일일코너: " + (f"**{dc[0]} → {dc[1]}** ({dc[2][5:]}~ 변경)" if dc else "변경 없음"))
    lines.append("- 주간 신설: " + (_corner_fmt(rev["new_c"], cap=6) if rev["new_c"] else "없음"))
    lines.append("- 주간 폐지: " + (_corner_fmt(rev["dropped_c"], cap=6) if rev["dropped_c"] else "없음"))
    lines.append("")
    lines.append("**② 사후 효과 (개편 전후 DAU)**")
    eff = rev["effect"]
    if eff and eff[0] != "short":
        b, a, pct, nb, na, anc = eff
        pct_s = f"{pct:+.1f}%" if pct is not None else "—"
        arrow = "▲" if (pct or 0) > 0 else ("▼" if (pct or 0) < 0 else "—")
        lines.append(f"- 개편 **{anc[5:]}** 기준 — 전 {nb}일 평균 **{b:,}명** → 후 {na}일 평균 **{a:,}명** ({arrow} {pct_s})")
    elif eff and eff[0] == "short":
        lines.append(f"- 개편({eff[1][5:]}) 직후라 사후 효과 판단엔 데이터가 부족합니다 (후 {eff[2]}일). 개편 1주 경과 후 재확인을 권장합니다.")
    else:
        lines.append("- 개편 기준일을 특정할 수 없어 사후 효과를 산정하지 못했습니다.")
    lines.append("")
    lines.append("※ 사후 효과는 요일 구성 편향을 막기 위해 개편 전후 각 1주 이상일 때만 산정합니다.")
    lines.append("")
    lines.append(_data_considerations_block("revision"))

    out = {
        "revision_detail_answer": "\n".join(lines),
        "channel_name": (user_context or {}).get("channel_name") or prev_context.get("channel_name"),
        "revision_code": code,
        # '주간 변화 살펴보기'(→2_cause) 등으로 넘어갈 때 분석 대상 프로그램 유지
        "top_change_program": name,
        "top_change_program_code": code,
    }
    chart = _build_dau_chart_data(name, hist)  # 프로그램 DAU 추이 (평일 토글 가능)
    if chart:
        out["chart_data"] = chart
    return out, False


# 슬롯 디스패치 테이블 (role, slot) → computer fn
# 일부 computer는 prev_context 가 필요 (예: 2_cause는 1_anchor의 top program code)
_SLOT_COMPUTERS = {
    ("CP", "1_anchor"): _compute_cp_anchor,
    ("CP", "3_adjacent"): _compute_cp_anchor_adjacent,
    ("CP", "4_revision"): _compute_cp_revision_effect,
    ("CP", "4_revision_detail"): _compute_cp_revision_detail,
    ("CP", "2_cause"): _compute_cp_cause,
    ("PD_PROGRAM", "1_anchor"): _compute_pd_program_anchor,
}


# ─── 내용 캐시 (이력 DB 재설계 §B — 1단계) ──────────────────────────────
# 같은 분석(프로그램·지표·기간·data_date)이면 경로(어떤 칩으로 왔는지) 무관하게 1회만 LLM 호출.
# data_date를 키에 포함 → 다음날 06:50 데이터 갱신 시 자동 만료.
try:
    from raas_history_db import cache_get as _cache_get, cache_put as _cache_put
except Exception:   # DB 미가용 환경(테스트 등)에서는 캐시 비활성
    _cache_get = _cache_put = None

# LLM polish가 무거운 슬롯만 캐싱 (프로그램 단위 — channel은 program에 내포)
_CACHE_SLOT_TYPES = {"cause", "impact"}
# 답변 콘텐츠/포맷 로직이 바뀌면 이 값을 올려 기존 캐시를 자동 무효화(수동 삭제 불필요).
_CACHE_VERSION = "2026-06-25e"


def _data_date() -> str:
    """캐시·무효화 기준일 — KPI 데이터의 최신 DATE."""
    rows = _kpi_rows()
    return max((r.get("DATE", "") for r in rows if r.get("DATE")), default="").replace("/", "-")


def _analysis_key(slot_type, channel_code, program_code,
                  metric, period, window, data_date) -> str:
    parts = [_CACHE_VERSION, slot_type, channel_code or "-", program_code or "-",
             metric or "-", period or "-", window or "-", data_date or "-"]
    return hashlib.sha1("|".join(parts).encode("utf-8")).hexdigest()[:16]


def _cached_compute(slot_type, key_inputs, compute):
    """compute: () -> (data, fb). 캐시 적중 시 LLM 우회.

    key_inputs: {program_code, metric, period, window, channel_code, data_date}
    """
    if not (_cache_get and _cache_put and slot_type in _CACHE_SLOT_TYPES):
        return compute()
    ddate = key_inputs.get("data_date") or _data_date()
    if not ddate:
        return compute()
    key = _analysis_key(slot_type, key_inputs.get("channel_code"),
                        key_inputs.get("program_code"), key_inputs.get("metric"),
                        key_inputs.get("period"), key_inputs.get("window"), ddate)
    try:
        cached = _cache_get(key)
    except Exception:
        cached = None
    if cached:
        try:
            data = json.loads(cached)
            data["_cache_hit"] = True
            data["_analysis_key"] = key
            return data, False
        except Exception:
            pass   # 손상된 캐시는 무시하고 재계산
    data, fb = compute()
    if not fb and isinstance(data, dict):
        data["_analysis_key"] = key
        data["_cache_hit"] = False
        try:
            payload = json.dumps(
                {k: v for k, v in data.items() if not k.startswith("_")},
                ensure_ascii=False)
            _cache_put(key, ddate, payload, slot_type=slot_type,
                       program_code=key_inputs.get("program_code"),
                       metric=key_inputs.get("metric"), period=key_inputs.get("period"),
                       window=key_inputs.get("window"), gen_tokens=len(payload) // 3)
        except Exception:
            pass
    return data, fb


# ─── v2 스키마 헬퍼 (토글 + 공통 출구) ───────────────────────────────────
def _is_v2(cfg: dict) -> bool:
    return (cfg or {}).get("schema_version") == "v2"


def _resolve_exits(cfg: dict, slot_cfg: dict) -> list[dict]:
    """슬롯의 exits id 목록 → common_exits 칩 리스트 (사본)."""
    ce = cfg.get("common_exits") or {}
    return [dict(ce[eid]) for eid in (slot_cfg.get("exits") or []) if eid in ce]


def _call_computer(fn, ctx: dict, prev: Optional[dict], chip_intent: Optional[str]):
    """computer 호출 — 시그니처 후방 호환 (3/2/1 인자)."""
    try:
        return fn(ctx, prev, chip_intent)
    except TypeError:
        try:
            return fn(ctx, prev)
        except TypeError:
            return fn(ctx)


def _reorder_entry_chips(cfg: dict, chips: list, ctx: dict) -> list:
    """entry.priority_rule(by=data_last_weekday) — 월요일→전주(WAU), 월초→전월(MAU) 칩 승격."""
    rule = (cfg.get("entry") or {}).get("priority_rule") or {}
    if rule.get("by") != "data_last_weekday":
        return chips
    last = ctx.get("data_last_date")
    if not last:
        return chips
    promote = rule.get("promote") or {}
    target = None
    try:
        from datetime import datetime
        d = datetime.strptime(last.replace("/", "-"), "%Y-%m-%d")
        if d.weekday() == 0 and promote.get("mon"):
            target = promote["mon"]
        elif d.day <= 7 and promote.get("month_start"):
            target = promote["month_start"]
    except Exception:
        return chips
    if not target:
        return chips
    return ([c for c in chips if c.get("intent") == target]
            + [c for c in chips if c.get("intent") != target])


# ─── 스토리라인 엔진 ──────────────────────────────────────────────────────
class StorylineEngine:
    def __init__(self, role: str, user: Optional[dict] = None,
                 channel_override: Optional[str] = None):
        self.role = role
        self.user = user or {}
        self.config = load_config(role)
        self._channel_override = channel_override

    def _user_context(self) -> dict:
        ctx: dict = {
            "user_name": self.user.get("name") or self.user.get("login_id") or "사용자",
        }
        role = self.config.get("role")

        # 데이터 신선도 — KPI CSV 최신 DATE 기준 (모든 직무 공통)
        try:
            from datetime import datetime
            rows = _kpi_rows()
            last_raw = max((r.get("DATE", "") for r in rows if r.get("DATE")), default="")
            if last_raw:
                ctx["data_last_date"] = last_raw.replace("/", "-")
                last_dt = datetime.strptime(last_raw.replace("-", "/"), "%Y/%m/%d")
                days_old = (datetime.now() - last_dt).days
                ctx["data_days_old"] = days_old
                if days_old > 2:
                    ctx["data_freshness_warning"] = (
                        f"⚠ **데이터 신선도 경고** — RAAS 보유 최신 데이터는 **{days_old}일 전**"
                        f"({ctx['data_last_date']} {_weekday_ko(last_raw)}요일) 기준입니다. "
                        f"`data/raas_kpi_latest.csv` 갱신 파이프라인(매일 06:50 예정) 점검이 필요합니다.\n\n"
                    )
                else:
                    ctx["data_freshness_warning"] = ""
        except Exception:
            pass

        # CP — 담당 채널 (D-015 옵션 A 이후: 사용자가 프로필에서 명시 선택)
        if role == "CP":
            if self._channel_override:
                ctx["channel_name"] = self._channel_override
            else:
                ch = _derive_cp_channel(self.user) if self.user else None
                if ch:
                    ctx["channel_name"] = ch
                # else: 미설정 — role_detect의 missing_setup이 안내

        # 제작PD — 담당 프로그램(my_programs[0])과 최근 회차 자동 채움
        elif role == "PD_PROGRAM":
            code = _derive_pd_primary_program(self.user) if self.user else None
            if code:
                row = _load_program_latest_row(code)
                if row:
                    ctx["program_code"] = code
                    ctx["program_name"] = row.get("PGM_NAME") or code
                    last_date = row.get("DATE", "")
                    ctx["last_episode_date"] = last_date.replace("/", "-")
                    ctx["weekday"] = _weekday_ko(last_date)
        return ctx

    def entry(self) -> dict:
        """첫 화면 — greeting + first_question + default chips."""
        ctx = self._user_context()
        entry_cfg = self.config["entry"]
        greeting = render(entry_cfg["greeting_template"], ctx)

        # CP인데 채널 미설정 — 셋업 안내 화면으로 대체
        if self.config["role"] == "CP" and not ctx.get("channel_name"):
            return {
                "ok": True,
                "role": self.config["role"],
                "role_name_ko": self.config["role_name_ko"],
                "greeting": greeting,
                "first_question": (
                    "**담당 채널이 아직 설정되지 않았습니다.**\n"
                    "사이드바의 **‘내 정보’**에서 담당 채널(파워FM 또는 러브FM)을 선택해 주세요."
                ),
                "chips": [],
                "user_context": ctx,
                "setup_required": True,
                "setup_action": "open_profile_modal",
            }

        return {
            "ok": True,
            "role": self.config["role"],
            "role_name_ko": self.config["role_name_ko"],
            "greeting": greeting,
            "first_question": render(entry_cfg["first_question"], ctx),
            "chips": (_reorder_entry_chips(self.config, entry_cfg["default_chips"], ctx)
                      if _is_v2(self.config) else entry_cfg["default_chips"]),
            "user_context": ctx,
        }

    def advance(self, slot_from: str, chip_intent: str,
                prev_context: Optional[dict] = None,
                next_slot_override: Optional[str] = None,
                toggle_state: Optional[dict] = None) -> dict:
        """칩 클릭 → 다음 슬롯 답변 렌더링.

        Args:
            slot_from: "entry" 또는 슬롯 ID ("1_anchor" 등)
            chip_intent: 클릭한 칩의 intent. "__toggle__"이면 같은 슬롯 재렌더(이동 아님).
            prev_context: 직전 advance가 돌려준 context_out
            next_slot_override: 동적 칩의 next_slot 직접 지정 (slot config에 없는 칩 지원)
            toggle_state: v2 토글 상태 {"metric":..,"period":..,"window":..}

        Returns:
            {ok, slot, slot_name, answer, fallback_used, chips_next, toggles, context_out}
        """
        prev_context = prev_context or {}
        cfg = self.config
        v2 = _is_v2(cfg)
        toggle_state = toggle_state or prev_context.get("toggle_state") or {}
        _reset_polish_usage()

        # 토글 변경 → 같은 슬롯 재렌더 (이동 step 아님)
        is_toggle = (chip_intent == "__toggle__")
        if is_toggle:
            clicked_chip = {"intent": "__toggle__", "next_slot": slot_from}
        elif next_slot_override:
            clicked_chip = {"intent": chip_intent, "next_slot": next_slot_override}
        else:
            if slot_from == "entry":
                chips = list(cfg["entry"]["default_chips"])
            else:
                slot_cfg_from = cfg["slots"].get(slot_from)
                if not slot_cfg_from:
                    return {"ok": False, "error": f"slot '{slot_from}' not found"}
                chips = list(slot_cfg_from.get("chips_next", []))
                if v2:                       # 공통 출구도 클릭 가능하도록 포함
                    chips += _resolve_exits(cfg, slot_cfg_from)
            clicked_chip = next(
                (c for c in chips if c.get("intent") == chip_intent), None
            )
            if not clicked_chip:
                return {
                    "ok": False,
                    "error": f"intent '{chip_intent}' not found in slot '{slot_from}'",
                    "available_intents": [c.get("intent") for c in chips],
                }

        next_slot = clicked_chip.get("next_slot")

        # 자유질의 출구 → 쿼리엔진/라우터에 위임, slot_from 유지(레일 복귀)
        if next_slot == "__freetext__":
            return {
                "ok": True, "freetext": True, "return_slot": slot_from,
                "toggle_state": toggle_state,
                "message": "자유질의를 쿼리엔진/라우터에 위임하고 직전 슬롯으로 복귀합니다.",
            }

        # _end: 산출물 종료
        if next_slot == "_end":
            output_id = clicked_chip.get("output_format_id", "")
            outputs = {o["id"]: o for o in cfg["ontology"]["outputsTo"]}
            o = outputs.get(output_id, {"name": output_id, "format": "?"})
            return {
                "ok": True, "slot": "_end", "ended": True,
                "output_format_id": output_id,
                "output_name": o.get("name", output_id),
                "output_format": o.get("format", ""),
                "message": f"{o.get('name', output_id)} 산출물 생성 — 리포트 엔진에서 변환",
            }

        slot_cfg = cfg["slots"].get(next_slot)
        if not slot_cfg:
            return {"ok": False, "error": f"next_slot '{next_slot}' not found"}

        slot_data, fallback_used = self._compute_slot_data(
            next_slot, prev_context, chip_intent=chip_intent, toggle_state=toggle_state
        )
        ctx = {**self._user_context(), **prev_context, **slot_data}
        ctx["toggle_state"] = toggle_state

        template_key = "fallback_answer_template" if fallback_used else "answer_template"
        template = slot_cfg.get(template_key) or slot_cfg.get("answer_template") or ""
        answer = render(template, ctx)

        # chips_next: 동적(computer) 우선, 없으면 정적 → v2는 공통 출구 보강
        if isinstance(slot_data, dict) and slot_data.get("chips_next"):
            chips_next = list(slot_data["chips_next"])
        else:
            chips_next = list(slot_cfg.get("chips_next", []))
        if v2:
            have = {c.get("intent") for c in chips_next}
            for e in _resolve_exits(cfg, slot_cfg):
                if e.get("intent") not in have:
                    chips_next.append(e)

        resp = {
            "ok": True,
            "slot": next_slot,
            "slot_name": slot_cfg.get("name"),
            "slot_purpose": slot_cfg.get("purpose"),
            "answer": answer,
            "fallback_used": fallback_used,
            "chips_next": chips_next,
            "toggles": slot_cfg.get("toggles"),
            "toggle_state": toggle_state,
            "context_out": ctx,
            "usage": _get_polish_usage(),
        }
        if is_toggle:
            resp["toggled"] = True       # 클라이언트: 이동 step으로 계상하지 말 것
        if isinstance(slot_data, dict) and slot_data.get("chart_data"):
            resp["chart_data"] = slot_data["chart_data"]
        if isinstance(slot_data, dict) and slot_data.get("kpi_meta"):
            resp["kpi_meta"] = slot_data["kpi_meta"]   # 클라 칩 재생성용 코드 맵
        return resp

    def _compute_slot_data(self, slot_id: str,
                           prev_context: dict,
                           chip_intent: Optional[str] = None,
                           toggle_state: Optional[dict] = None) -> tuple[dict, bool]:
        """슬롯별 데이터 계산.

        v2(CP) 토글 슬롯은 toggle_state로 기존 computer에 라우팅. 그 외는 _SLOT_COMPUTERS.
        """
        cfg = self.config
        role = cfg.get("role")
        ctx = self._user_context()
        if _is_v2(cfg) and role == "CP":
            res = self._cp_v2_compute(slot_id, ctx, prev_context,
                                      chip_intent, toggle_state or {})
            if res is not None:
                return res
        computer = _SLOT_COMPUTERS.get((role, slot_id))
        if computer:
            return _call_computer(computer, ctx, prev_context, chip_intent)
        return {}, True

    def _cp_v2_compute(self, slot_id, ctx, prev_context, chip_intent, ts):
        """v2 CP 슬롯 → 토글 상태로 기존 computer에 라우팅. 미처리 슬롯은 None 반환."""
        if slot_id == "1_anchor":
            metric = ts.get("metric", "scale")
            period = ts.get("period", "day")
            if metric in ("deep", "retention", "new_churn"):
                # 다른 지표 큰 변화 → 해당 지표 변화순 정렬 (지표별로 결과 차별화)
                return _compute_cp_anchor_metric(ctx, prev_context, metric)
            ci = {"week": "show_weekly_top", "month": "show_monthly_top"}.get(period)
            data, fb = _call_computer(_compute_cp_anchor, ctx, prev_context, ci)
            if isinstance(data, dict):
                data.pop("chips_next", None)   # v2 정적 칩 + 공통 출구 사용
            return data, fb
        if slot_id == "2_cause":
            period = ts.get("period", "day")
            fn = {"week": _compute_cp_cause_weekly,
                  "month": _compute_cp_cause_monthly}.get(period, _compute_cp_cause)
            pc = prev_context or {}
            key_inputs = {
                "program_code": pc.get("top_change_program_code"),
                "metric": {"week": "WAU", "month": "MAU"}.get(period, "DAU"),
                "period": period, "data_date": _data_date(),
            }
            return _cached_compute(
                "cause", key_inputs,
                lambda: _call_computer(fn, ctx, prev_context, chip_intent))
        if slot_id == "3_adjacent":
            # 인접 비교도 첫 화면에서 고른 기간(toggle_state) 그대로 이어감
            pc = dict(prev_context or {})
            _am = {"day": "pct", "week": "weekly_pct", "month": "monthly_pct"}.get(ts.get("period"))
            if _am:
                pc["anchor_mode"] = _am
            data, fb = _call_computer(_compute_cp_anchor_adjacent, ctx, pc, chip_intent)
            if isinstance(data, dict):
                data.pop("chips_next", None)
            return data, fb
        if slot_id == "4_revision":
            return _call_computer(_compute_cp_revision_effect, ctx, prev_context, chip_intent)
        if slot_id == "4_revision_detail":
            pc = dict(prev_context or {})
            pc["window"] = ts.get("window", "4w")
            data, fb = _call_computer(_compute_cp_revision_detail, ctx, pc, chip_intent)
            if isinstance(data, dict):
                data.pop("chips_next", None)
            return data, fb
        if slot_id == "1_kpi":
            return _compute_cp_kpi_table(ctx, prev_context, ts)
        if slot_id == "2_impact":
            pc = prev_context or {}
            key_inputs = {
                "program_code": pc.get("top_change_program_code"),
                # focus 지표(kpi_metric) 우선, 없으면 범주로 키 구분(범주는 저장 안 하지만 키 내부엔 사용)
                "metric": pc.get("kpi_metric") or ts.get("category") or "scale",
                "period": ts.get("period", "day"), "data_date": _data_date(),
            }
            return _cached_compute(
                "impact", key_inputs,
                lambda: _compute_cp_metric_impact(ctx, prev_context, ts, chip_intent))
        if slot_id in ("3_context", "5_closing"):
            return {}, False   # 템플릿(answer_template) 사용
        return None

    def peek_chips(self, slot_id: str, toggle_state: Optional[dict] = None) -> list:
        """슬롯의 정적 chips_next + 공통 출구만 반환 (무거운 computer 호출 없음).
        자유질의 복귀 시 레일(칩) 재구성용."""
        cfg = self.config
        sc = cfg.get("slots", {}).get(slot_id) or {}
        chips = list(sc.get("chips_next", []))
        if _is_v2(cfg):
            have = {c.get("intent") for c in chips}
            for e in _resolve_exits(cfg, sc):
                if e.get("intent") not in have:
                    chips.append(e)
        return chips


# ─── Export stub (Phase 3 ⑥ 예정) ───────────────────────────────────────
def export_stub(role: str, output_format_id: str,
                slots_visited: list) -> dict:
    """산출물 생성 — 현재 메타데이터만 반환. PPT/카톡 변환은 Phase 3 ⑥."""
    cfg = load_config(role)
    outputs = {o["id"]: o for o in cfg["ontology"]["outputsTo"]}
    o = outputs.get(output_format_id)
    if not o:
        return {
            "ok": False,
            "error": f"unknown output_format_id '{output_format_id}'",
            "available": list(outputs.keys()),
        }
    return {
        "ok": True,
        "stub": True,
        "role": role,
        "output_format_id": output_format_id,
        "output_name": o.get("name"),
        "output_format": o.get("format"),
        "slots_visited": [s.get("slot") for s in slots_visited],
        "message": (
            f"{o.get('name')} 산출물 생성 요청 접수 — "
            "실제 변환(PPT/카톡 등)은 Phase 3 ⑥에서 구현됩니다."
        ),
    }


# ─── CLI 자가 테스트 ─────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys

    print("=== 사용 가능한 직무 ===")
    print(list_available_roles())
    print()

    # 시연용 가상 user
    fake_user = {
        "name": "박CP",
        "login_id": "cp_powerfm",
        "role": "CP",
        "channel": "파워FM",
        "my_programs": [],
    }

    print("=== role_detect ===")
    rd = role_detect(fake_user)
    print(json.dumps(rd, ensure_ascii=False, indent=2))
    print()

    print("=== entry (CP) ===")
    engine = StorylineEngine(role="cp", user=fake_user)
    e = engine.entry()
    print(json.dumps(e, ensure_ascii=False, indent=2))
    print()

    print("=== advance: entry → 1_anchor (intent=show_biggest_change) ===")
    adv1 = engine.advance("entry", "show_biggest_change")
    print(json.dumps({k: v for k, v in adv1.items() if k != "context_out"},
                     ensure_ascii=False, indent=2))
    print()

    print("=== advance: 1_anchor → 2_cause (intent=explain_change) ===")
    adv2 = engine.advance("1_anchor", "explain_change",
                          prev_context=adv1.get("context_out", {}))
    print(json.dumps({k: v for k, v in adv2.items() if k != "context_out"},
                     ensure_ascii=False, indent=2))
