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
    "cp": "cp.json",
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
                          events: Optional[list[dict]] = None) -> str:
    """온톨로지(TTL) 기반 원시 분석문. 평상시 대비 비교 위주 해석.

    LLM polish가 톤·구조를 다듬어 최종 출력으로 변환.
    (제목 헤더 ### 사용 안 함 — 사용자 요청)
    """
    decomps = _query_decompositions()
    if not decomps:
        return "(원인 분석 온톨로지를 로드할 수 없습니다. rdflib + raas_onto/*.ttl 확인)"

    lines = []
    lines.append(
        f"**{program_name}**의 일간활성사용자가 **전주 대비 {change_pct:+.1f}%** 변화한 내용을 분석합니다."
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
            if flow.get("ok"):
                # 등식형 한 줄 — 직관적 (직전일 대비)
                new_v = flow["new"]; react_v = flow["react"]; churn_v = flow["churn_estimate"]
                delta = flow["delta_dau"]
                eqn = (f"- ΔDAU(직전일 대비) = 신규 + 복귀 − 이탈 = "
                       f"**{new_v:+,}** + **{react_v:+,}** − **{churn_v:,}** = "
                       f"**{delta:+,}명**")
                lines.append(eqn)

                # 잔차 → 작은 마커
                if abs(flow.get("discrepancy", 0)) >= 3:
                    lines.append("[mini]💡 복귀사용자 데이터 안정화 중[/mini]")

                lines.append("[mini]ⓘ 참고용 — 위 흐름은 직전일 대비 수치라 전주 동요일 대비와 시점이 다릅니다[/mini]")

                # 주된 요인 (신규 유입 위주)
                bd = flow.get("biggest_driver")
                if bd and bd.get("baseline") is not None:
                    bl_v = bd["baseline"]
                    label = bd["label"]
                    diff = bd["diff"]
                    if bl_v and abs(diff) / max(abs(bl_v), 1) >= 0.10:
                        act = "더 많음" if diff > 0 else "더 적음"
                        base_fmt = (f"{bl_v:,}" if label == "이탈" else f"+{bl_v:,}")
                        pct = round(abs(diff) / max(abs(bl_v), 1) * 100)
                        lines.append("")
                        lines.append(
                            f"직전일 대비 흐름의 주된 요인은 **{label}**입니다. "
                            f"평상시({base_fmt}) 대비 **{abs(diff):,}명 {act} ({pct}%)**."
                        )
            else:
                lines.append(f"- ⚠ 데이터 부족 — {', '.join(flow.get('missing', []))}")

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
                lines.append("- 📊 **현재 데이터 부족** — 변화가 어느 사용자 층에서 발생했는지 확인 불가")
                lines.append("")
                lines.append("**추가 적재가 필요한 Splunk 데이터:**")
                for h in hints:
                    lines.append(f"- `{h['key']}` ({h['label']})")
                    lines.append(f"  ↳ `{h['splunk_spl']}`")
                lines.append("")
                lines.append("적재 완료 후엔 성별/연령/시간대별 변화 분해가 자동으로 가능합니다.")

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
        })
    return programs


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
            "next_slot": "4_loop",
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


def _compute_cp_anchor_scan(user_context: dict,
                            prev_context: Optional[dict] = None,
                            chip_intent: Optional[str] = None) -> tuple[dict, bool]:
    """CP 1_anchor_scan — 채널 전 프로그램 4단면 + 위험 진입 스캔.

    출력: scan_answer (markdown 본문) + 동적 chips_next (등장 빈도순 TOP 5).
    """
    prev_context = prev_context or {}
    channel = (user_context or {}).get("channel_name") or \
              prev_context.get("channel_name") or ""
    if not channel:
        return {"scan_answer": "담당 채널이 결정되지 않았습니다."}, True

    programs = _load_scan_program_kpis(channel)
    if not programs:
        return {"scan_answer": f"**{channel}** 채널의 프로그램 KPI를 찾을 수 없습니다."}, True

    last_date = (programs[0].get("DATE") or "").replace("/", "-")
    weekday = _weekday_ko(programs[0].get("DATE", ""))

    # 섹션별 TOP
    scale_rows   = _scan_top_per_metric(programs, _SCAN_METRICS_SCALE)
    flow_rows    = _scan_top_per_metric(programs, _SCAN_METRICS_FLOW)
    quality_rows = _scan_top_per_metric(programs, _SCAN_METRICS_QUALITY)
    ret_overall  = _scan_top_retention(programs, _SCAN_RETENTION_OVERALL, "전체")
    ret_new      = _scan_top_retention(programs, _SCAN_RETENTION_NEW, "신규")
    at_risk      = _scan_detect_at_risk(programs)

    # 본문 조립
    lines: list[str] = []
    lines.append(f"**{channel} 전체 프로그램 스캔** — {last_date} {weekday}요일 기준")
    lines.append("")

    def _render_section(emoji_title: str, rows: list[dict]):
        if not rows:
            return
        lines.append(f"{emoji_title}")
        lines.append("| 지표 | 가장 큰 변화 프로그램 | 이번 수치 | 전주 대비 |")
        lines.append("|---|---|---|---|")
        for r in rows:
            cur = _scan_format_current(r["current"], r["current_unit"])
            chg = _scan_format_value(r["change"], r["change_unit"])
            lines.append(f"| {r['label']} | {r['name']} | {cur} | {chg} |")
        lines.append("")

    _render_section("📏 **사용자 규모 변화**",   scale_rows)
    _render_section("🔄 **사용자 흐름 변화**",   flow_rows)
    _render_section("🎧 **청취 품질 변화 (전주 대비)**", quality_rows)

    # 유지율 — 2줄 표
    if ret_overall or ret_new:
        lines.append("🔁 **유지율 변화 (코호트 × 기간)**")
        lines.append("| 코호트 | 기간 | 프로그램 | 이번 수치 | 전주 대비 |")
        lines.append("|---|---|---|---|---|")
        for r in (ret_overall, ret_new):
            if not r:
                continue
            cur = _scan_format_current(r["current"], r["current_unit"])
            chg = _scan_format_value(r["change"], r["change_unit"])
            lines.append(f"| {r['cohort']} | {r['period']} | {r['name']} | {cur} | {chg} |")
        lines.append("")

    # 위험 범위
    if at_risk:
        lines.append(f"⚠ **위험 범위 진입 ({len(at_risk)}건)**")
        for ar in at_risk[:6]:
            kinds = ", ".join(rs["kind"] for rs in ar["reasons"])
            details = " · ".join(rs["detail"] for rs in ar["reasons"])
            lines.append(f"- **{ar['code']} {ar['name']}**: {details} _({kinds})_")
        lines.append("")
    else:
        lines.append("⚠ 위험 범위 진입 프로그램은 없습니다.")
        lines.append("")

    lines.append("어떤 프로그램을 살펴볼까요?")

    answer = "\n".join(lines)

    # 동적 칩 — 본문 등장 프로그램 빈도순
    appearance: list[str] = []
    name_by_code: dict[str, str] = {}
    for r in (scale_rows + flow_rows + quality_rows):
        appearance.append(r["code"])
        name_by_code[r["code"]] = r["name"]
    for r in (ret_overall, ret_new):
        if r:
            appearance.append(r["code"])
            name_by_code[r["code"]] = r["name"]
    at_risk_codes: list[str] = []
    for ar in at_risk:
        appearance.append(ar["code"])
        name_by_code[ar["code"]] = ar["name"]
        at_risk_codes.append(ar["code"])

    chips = _scan_build_chips(appearance, channel, at_risk_codes, name_by_code)
    # 끝에 직접 입력 안내 칩
    chips.append({
        "label": "💬 직접 입력",
        "intent": "scan_free_input",
        "next_slot": "1_anchor_scan",  # 같은 슬롯 유지 (실제로는 클라이언트가 입력창 포커스)
        "payload": {},
    })

    return {"scan_answer": answer, "chips_next": chips}, False


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


def _build_dau_chart_data(program_name: str, history: list, days: int = 28) -> Optional[dict]:
    """일별 DAU 추이 → chart_data (timeseries). 직전 N일."""
    if not history:
        return None
    recent = history[-days:] if len(history) > days else history
    points = []
    for r in recent:
        d = (r.get("DATE") or "").replace("/", "-")
        v = _safe_float(r.get("dau"))
        if d and v is not None:
            points.append({"date": d, "value": int(v)})
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
        lines.append(f"**{program_name}**의 WAU가 **전주 대비 {wau_chg:+.1f}%** 변화했습니다 (WAU **{int(wau):,}명**).")
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

    # ② 사용자 흐름 분해 (주간)
    lines.append("**② 사용자 흐름 분해 (전주 대비)**")
    if (new_week is not None and react_week is not None and churn_week is not None
            and wau_prev is not None and wau is not None):
        churn_count = round(wau_prev * churn_week / 100)
        delta_wau = int(wau - wau_prev)
        lines.append(
            f"- ΔWAU(전주 대비) = 신규 + 복귀 − 이탈 ≈ "
            f"**{int(new_week):+,}** + **{int(react_week):+,}** − **{churn_count:,}** = **{delta_wau:+,}명**"
        )
        # 평상시(전주) 대비
        comps = []
        if new_week_prev is not None and new_week_prev != 0:
            diff = int(new_week) - int(new_week_prev)
            pct = round(diff / max(abs(new_week_prev), 1) * 100)
            d = "더 많이" if diff > 0 else "더 적게"
            comps.append(f"  - **신규**({int(new_week):+,}): 전주(+{int(new_week_prev):,}) 대비 **{abs(diff):,}명 {d}** ({pct:+}%)")
        if react_week_prev is not None and react_week_prev != 0:
            diff = int(react_week) - int(react_week_prev)
            pct = round(diff / max(abs(react_week_prev), 1) * 100)
            d = "더 많이" if diff > 0 else "더 적게"
            comps.append(f"  - **복귀**({int(react_week):+,}): 전주(+{int(react_week_prev):,}) 대비 **{abs(diff):,}명 {d}** ({pct:+}%)")
        if churn_week_diff is not None:
            d = "상승" if churn_week_diff > 0 else "하락"
            comps.append(f"  - **이탈율**({churn_week:.1f}%): 전주 대비 **{churn_week_diff:+.1f}pp {d}**")
        lines.extend(comps)
    else:
        lines.append("- ⚠ 흐름 분해에 필요한 주간 데이터 일부 부족")
    lines.append("[mini]💡 복귀사용자 데이터 안정화 중[/mini]")
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
        lines.append(f"**{program_name}**의 MAU가 **전월 대비 {mau_chg:+.1f}%** 변화했습니다 (MAU **{int(mau):,}명**).")
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

    # ② 월간 흐름 분해
    lines.append("**② 사용자 흐름 분해 (전월 대비)**")
    if (new_mon is not None and react_mon is not None and churn_mon is not None
            and mau_prev is not None and mau is not None):
        churn_count = round(mau_prev * churn_mon / 100)
        delta_mau = int(mau - mau_prev)
        lines.append(
            f"- ΔMAU(전월 대비) = 신규 + 복귀 − 이탈 ≈ "
            f"**{int(new_mon):+,}** + **{int(react_mon):+,}** − **{churn_count:,}** = **{delta_mau:+,}명**"
        )
        if new_mon_prev is not None and new_mon_prev != 0:
            diff = int(new_mon) - int(new_mon_prev)
            pct = round(diff / max(abs(new_mon_prev), 1) * 100)
            d = "더 많이" if diff > 0 else "더 적게"
            lines.append(f"  - **신규**({int(new_mon):+,}): 전월(+{int(new_mon_prev):,}) 대비 **{abs(diff):,}명 {d}** ({pct:+}%)")
    else:
        lines.append("- ⚠ 월간 흐름 데이터 부족")
    lines.append("[mini]💡 복귀사용자 데이터 안정화 중[/mini]")
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
    chart = _build_wau_chart_data(program_name, history)
    out = {"cause_explanation": polished}
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
    chart = _build_mau_chart_data(program_name, history)
    out = {"cause_explanation": polished}
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

    chart = _build_dau_chart_data(program_name, history)
    out = {"cause_explanation": polished}
    if chart:
        out["chart_data"] = chart
    return out, False


def _compute_cp_anchor_more(user_context: dict,
                            prev_context: Optional[dict] = None,
                            chip_intent: Optional[str] = None) -> tuple[dict, bool]:
    """CP 1_anchor_more — 1_anchor의 TOP3를 제외한 나머지 프로그램을 변화 순위로 나열.

    prev_context["anchor_mode"]에 따라 지표 선택:
        'pct' / None  → DAU 전주 동요일 대비 (변동폭)
        'abs'         → DAU 변동수치
        'weekly'      → WAU 전주 대비
        'monthly'     → MAU 전월 대비
    """
    prev_context = prev_context or {}
    channel = (user_context or {}).get("channel_name") or prev_context.get("channel_name")
    if not channel:
        return {"more_answer": "담당 채널이 결정되지 않았습니다."}, True
    programs = _load_latest_program_kpis(channel)
    if not programs:
        return {"more_answer": f"**{channel}** 채널의 프로그램 KPI를 찾을 수 없습니다."}, True

    raw_mode = (prev_context.get("anchor_mode") or "pct").lower()
    # 호환성: 'weekly' → 'weekly_pct', 'monthly' → 'monthly_pct'
    if raw_mode == "weekly":  raw_mode = "weekly_pct"
    if raw_mode == "monthly": raw_mode = "monthly_pct"
    rows = _kpi_rows()
    last_date = max((r.get("DATE", "") for r in rows if r.get("DATE")), default="").replace("/", "-")

    def _abs_diff_dau(p):
        c = p.get("dau"); pr = p.get("dau_prev_wow")
        return (c - pr) if (c is not None and pr is not None) else None

    if raw_mode in ("weekly_pct", "weekly_abs"):
        val_f, prev_f, chg_f = "wau", "wau_prev", "wau_chg"
        metric_label, compare_ko = "WAU", "전주 대비"
        sort_pct = raw_mode == "weekly_pct"
    elif raw_mode in ("monthly_pct", "monthly_abs"):
        val_f, prev_f, chg_f = "mau", "mau_prev", "mau_chg"
        metric_label, compare_ko = "MAU", "전월 대비"
        sort_pct = raw_mode == "monthly_pct"
    else:  # 'pct' (DAU 변동폭) or 'abs' (DAU 변동수치)
        val_f, prev_f, chg_f = "dau", "dau_prev_wow", "dau_wow_chg"
        metric_label, compare_ko = "DAU", "전주 동요일 대비"
        sort_pct = raw_mode == "pct"

    have = [p for p in programs if p.get(chg_f) is not None and p.get(val_f) is not None]
    if sort_pct:
        have.sort(key=lambda p: abs(p[chg_f]), reverse=True)
    else:
        def _delta(p):
            v = p.get(val_f); pr = p.get(prev_f)
            return (v - pr) if (v is not None and pr is not None) else 0
        have = [p for p in have if p.get(prev_f) is not None]
        have.sort(key=lambda p: abs(_delta(p)), reverse=True)

    rest = have[3:]
    if not rest:
        return {"more_answer": f"**{channel}**에서 TOP 3 외 표시할 프로그램이 없습니다."}, False

    lines: list[str] = []
    sort_basis = "변동폭" if sort_pct else "변동수치"
    lines.append(f"**{channel}**의 TOP 3 외 {len(rest)}개 프로그램 — **{metric_label} {compare_ko}** {sort_basis} 순")
    # 모드별 기준일 라벨 (첫화면 인사말과 동일 포맷)
    from datetime import datetime, timedelta
    period_label_ko = ""
    if last_date:
        try:
            ld = datetime.strptime(last_date, "%Y-%m-%d")
            if raw_mode in ("weekly_pct", "weekly_abs"):
                # 지난주 Mon~Sun (Mon=0, Sun=6 in weekday())
                this_mon = ld - timedelta(days=ld.weekday())
                last_mon = this_mon - timedelta(days=7)
                last_sun = this_mon - timedelta(days=1)
                period_label_ko = f"지난주 ({last_mon.strftime('%Y-%m-%d')} ~ {last_sun.strftime('%Y-%m-%d')})"
            elif raw_mode in ("monthly_pct", "monthly_abs"):
                # 지난달 (YYYY년 M월)
                first_this = ld.replace(day=1)
                last_month_last = first_this - timedelta(days=1)
                period_label_ko = f"지난달 ({last_month_last.year}년 {last_month_last.month}월)"
            else:
                period_label_ko = f"기준일: {last_date}"
        except Exception:
            period_label_ko = f"기준일: {last_date}"
    if period_label_ko:
        lines.append(f"_({period_label_ko})_")
    lines.append("")
    for i, p in enumerate(rest, start=4):
        val = p.get(val_f)
        prev = p.get(prev_f)
        chg = p.get(chg_f)
        if val is None or chg is None:
            continue
        v_int = int(val)
        if prev is not None:
            p_int = int(prev)
            diff = v_int - p_int
            lines.append(f"{i}. {p['name']}: {p_int:,}명 → {v_int:,}명 ({diff:+,}명, {chg:+.1f}%)")
        else:
            lines.append(f"{i}. {p['name']}: {chg:+.1f}%")

    lines.append("")
    lines.append("어떤 프로그램을 살펴볼까요?")

    data = {
        "more_answer": "\n".join(lines),
        "channel_name": channel,
        "anchor_mode": raw_mode,
    }
    return data, False


# 슬롯 디스패치 테이블 (role, slot) → computer fn
# 일부 computer는 prev_context 가 필요 (예: 2_cause는 1_anchor의 top program code)
_SLOT_COMPUTERS = {
    ("CP", "1_anchor"): _compute_cp_anchor,
    ("CP", "1_anchor_scan"): _compute_cp_anchor_scan,
    ("CP", "1_anchor_more"): _compute_cp_anchor_more,
    ("CP", "2_cause"): _compute_cp_cause,
    ("CP", "2_cause_weekly"): _compute_cp_cause_weekly,
    ("CP", "2_cause_monthly"): _compute_cp_cause_monthly,
    ("PD_PROGRAM", "1_anchor"): _compute_pd_program_anchor,
}


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
            "chips": entry_cfg["default_chips"],
            "user_context": ctx,
        }

    def advance(self, slot_from: str, chip_intent: str,
                prev_context: Optional[dict] = None,
                next_slot_override: Optional[str] = None) -> dict:
        """칩 클릭 → 다음 슬롯 답변 렌더링.

        Args:
            slot_from: "entry" 또는 슬롯 ID ("1_anchor" 등)
            chip_intent: 클릭한 칩의 intent 값
            prev_context: 직전 advance가 돌려준 context_out
            next_slot_override: 동적 칩의 next_slot을 직접 지정 (slot config에 없는 칩 지원)

        Returns:
            {ok, slot, slot_name, answer, fallback_used, chips_next, context_out}
        """
        prev_context = prev_context or {}

        # 동적 칩 → next_slot_override가 있으면 chips 매칭 우회
        if next_slot_override:
            clicked_chip = {"intent": chip_intent, "next_slot": next_slot_override}
        else:
            # slot_from에서 칩 목록 찾기
            if slot_from == "entry":
                chips = self.config["entry"]["default_chips"]
            else:
                slot_cfg_from = self.config["slots"].get(slot_from)
                if not slot_cfg_from:
                    return {"ok": False, "error": f"slot '{slot_from}' not found"}
                chips = slot_cfg_from.get("chips_next", [])

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

        # _end: 산출물 종료 (Phase 3 ⑥에서 본격 구현)
        if next_slot == "_end":
            output_id = clicked_chip.get("output_format_id", "")
            outputs = {o["id"]: o for o in self.config["ontology"]["outputsTo"]}
            o = outputs.get(output_id, {"name": output_id, "format": "?"})
            return {
                "ok": True,
                "slot": "_end",
                "ended": True,
                "output_format_id": output_id,
                "output_name": o.get("name", output_id),
                "output_format": o.get("format", ""),
                "message": (
                    f"{o.get('name', output_id)} 산출물 생성 — "
                    "Phase 3 ⑥에서 실제 변환 구현 예정"
                ),
            }

        # 다음 슬롯 렌더링
        slot_cfg = self.config["slots"].get(next_slot)
        if not slot_cfg:
            return {"ok": False, "error": f"next_slot '{next_slot}' not found"}

        slot_data, fallback_used = self._compute_slot_data(
            next_slot, prev_context, chip_intent=chip_intent
        )
        ctx = {**self._user_context(), **prev_context, **slot_data}

        template_key = (
            "fallback_answer_template" if fallback_used else "answer_template"
        )
        template = (
            slot_cfg.get(template_key) or slot_cfg.get("answer_template") or ""
        )
        answer = render(template, ctx)

        # 동적 칩 — slot_data["chips_next"]가 있으면 cp.json의 chips_next 덮어쓰기
        if isinstance(slot_data, dict) and slot_data.get("chips_next"):
            chips_next = slot_data["chips_next"]
        else:
            chips_next = slot_cfg.get("chips_next", [])

        resp = {
            "ok": True,
            "slot": next_slot,
            "slot_name": slot_cfg.get("name"),
            "slot_purpose": slot_cfg.get("purpose"),
            "answer": answer,
            "fallback_used": fallback_used,
            "chips_next": chips_next,
            "context_out": ctx,
        }
        # slot computer가 chart_data를 만들었으면 응답에 포함 (클라이언트가 ECharts로 렌더)
        if isinstance(slot_data, dict) and slot_data.get("chart_data"):
            resp["chart_data"] = slot_data["chart_data"]
        return resp

    def _compute_slot_data(self, slot_id: str,
                           prev_context: dict,
                           chip_intent: Optional[str] = None) -> tuple[dict, bool]:
        """슬롯별 데이터 계산. _SLOT_COMPUTERS에 등록된 함수만 본격 동작.

        computer 시그니처 후방 호환:
            (user_context, prev_context=None, chip_intent=None)
            (user_context, prev_context=None)
            (user_context)
        """
        computer = _SLOT_COMPUTERS.get((self.config["role"], slot_id))
        if computer:
            ctx = self._user_context()
            try:
                return computer(ctx, prev_context, chip_intent)
            except TypeError:
                try:
                    return computer(ctx, prev_context)
                except TypeError:
                    return computer(ctx)
        return {}, True


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
