"""raas_metrics_engine.py — grounding이 재사용하는 KPI 파생지표 계산 계층.

구 CP 다중슬롯 스토리라인 오케스트레이션은 은퇴·제거됨. 현재 역할:
    - 데이터 계산: _compute_flow_decomposition / _compute_cohort / _compute_stickiness /
      _compute_programming_impact / _compute_weekday_pattern_check / _detect_program_revision
    - 룩업·유틸: _kpi_rows / _load_program_latest_row / _load_program_history /
      build_program_schedule / build_query_provenance / is_schedule_query / _CHANNEL_CODE
호출자: raas_grounding(provider fetch), raas_server(편성표·provenance·이상탐지).
"""
from __future__ import annotations

import csv
import hashlib
import json
import re
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).parent
ONTO_DIR = ROOT / "raas_onto"   # 형식 TTL 온톨로지 (raas_ontology_*.ttl)
KPI_CSV = ROOT / "data" / "raas_kpi_latest.csv"

# 프로그램 코드 첫 글자 → 채널명
PROGRAM_PREFIX_TO_CHANNEL = {
    "F": "파워FM",
    "L": "러브FM",
    "M": "러브FM",  # M05~M11도 러브FM 소속
    "G": "고릴라M",
    "P": "픽채널",
    "T": "전체",
}









# ─── 사용자 → 스토리라인 직무 매핑 ──────────────────────────────────────








# ─── 데이터 소스 — Splunk 우선, CSV는 fallback ────────────────────
# raas_server가 부팅 시 timeline provider 등록 (get_cached_timeline)
# 이게 없으면 (CLI/테스트 환경) CSV 직접 읽기로 폴백.
_TIMELINE_PROVIDER = None  # type: ignore


def set_timeline_provider(fn) -> None:
    """raas_server.py에서 호출: METRICS.set_timeline_provider(get_cached_timeline).

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




# ─── 원인 분석 온톨로지 로더 (RDF/Turtle, D-016) ─────────────────
# raas_onto/raas_ontology_fields.ttl + raas_ontology_cause.ttl 결합
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
    for fname in ("raas_ontology_fields.ttl", "raas_ontology_cause.ttl", "raas_ontology_calendar.ttl"):
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




# ─── Claude API 다듬기 헬퍼 ─────────────────────────────────────
_NO_HRULE_RULE = (
    "마크다운 가로줄(`---`, `***`, `___`)을 절대 사용하지 마세요. "
    "섹션 사이는 빈 줄 1개로만 구분합니다."
)




# 스토리라인 1회 advance 동안 _llm_polish가 쓴 토큰 누적기 (서버 단일스레드 전제)
_POLISH_USAGE = {"input": 0, "output": 0}








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




# 기간별 비율 컬럼: (신규비율, 복귀율, 이탈율)
_RATE_LENS_COLS = {
    "day":  ("new_share",      "react_rate",      "churn_rate"),
    "week": ("new_week_share", "react_rate_week", "churn_rate_week"),
    "mon":  ("new_mon_share",  "react_rate_mon",  "churn_rate_mon"),
}




# 기간별 레벨 분해 설정: (DAU컬럼, 신규컬럼, 복귀컬럼, 비교offset일, 라벨, 이탈율컬럼, 이탈base컬럼)
_FLOW_PERIOD = {
    "day":  ("dau", "new",      "react",      7,  "전주 동요일", "churn_rate",      "dau_d2"),
    "week": ("wau", "new_week", "react_week", 7,  "전주",        "churn_rate_week", "wau_prev"),
    "mon":  ("mau", "new_mon",  "react_mon",  30, "전월",        "churn_rate_mon",  "mau_prev"),
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








# ─── KPI 데이터 로더 (Phase 1 ② 즉시 동작 가능) ────────────────────────


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






# ─── 슬롯 데이터 컴퓨터 (디스패치) ───────────────────────────────────────






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






_ASPECT_KO = {"rate": "변화율", "abs": "변화량", "value": "현재값"}
# 기간 맞춤 활성사용자 — (값필드, 라벨)
_AU_FIELD = {"day": ("dau", "DAU"), "week": ("wau", "WAU"), "month": ("mau", "MAU")}










# ─── 주간·월간 원인 분석 헬퍼 ──────────────────────────────────























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








# 슬롯 디스패치 테이블 (role, slot) → computer fn
# 일부 computer는 prev_context 가 필요 (예: 2_cause는 1_anchor의 top program code)


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








# ─── v2 스키마 헬퍼 (토글 + 공통 출구) ───────────────────────────────────








# ─── 스토리라인 엔진 ──────────────────────────────────────────────────────


# ─── 이상탐지 (구 raas_fallback_engine에서 이관) ─────────────────────────
#    ZScoreDetector 기반 알림 — 서버 get_cached_anomalies가 호출(추천칩·digest 공유).
from raas_datasource import get_available_dates as _get_available_dates

def _i2(v, d=0):
    try: return int(float(v)) if v not in (None, '', 'None', 'null') else d
    except Exception: return d

def _fn2(v):
    try: return float(v) if v not in (None, '', 'None', 'null') else None
    except Exception: return None

def _pgm_name(code, row=None, default=None):
    """코드→표시 이름 (TTL 어댑터 → row.pgm_name → code). 서버 KPI 패널·이상탐지 공용."""
    try:
        from raas_onto import get_adapter
        label = get_adapter()._onto.label_ko(f"raas:{code}")
        if label and label != f"raas:{code}":
            return label
    except Exception:
        pass
    if row is not None:
        nm = (row.get('pgm_name') or '').strip()
        if nm:
            return nm
    return default if default is not None else code

def _build_alert_kpi(row: dict) -> dict:
    return {
        'dau_chg':         _fn2(row.get('dau_chg')),
        'deep_rate_diff':  _fn2(row.get('deep_rate_diff')),
        'new_chg':         _fn2(row.get('new_chg')),
        'churn_rate_diff': _fn2(row.get('churn_rate_diff')),
        'react_rate':      _fn2(row.get('react_rate')),
        'habit_rate':      _fn2(row.get('habit_rate')),
    }

def _evaluate_alerts(row: dict, timeline_snap: dict, latest_dt: str) -> dict:
    """Z-score 기반 알림 평가 (우선) + 고정룰 fallback + 위험 프로그램 감지."""
    alerts = []
    try:
        from raas_onto import get_adapter
        adapter = get_adapter()
        alerts = adapter.evaluate_zscore_alerts(timeline_snap, latest_dt)

        # 프로그램명을 알림에 결정적으로 주입 — 코드만 있으면 LLM이 표 렌더 중
        # 코드→이름 조인을 스스로 하다 오귀속(예: F05를 씨네타운으로) 발생.
        for a in alerts:
            _c = a.get('code')
            if _c and not a.get('program'):
                _nm = _pgm_name(_c)
                if _nm and _nm != _c:
                    a['program'] = _nm
                    if a.get('msg') and f"[{_c}]" in a['msg']:
                        a['msg'] = a['msg'].replace(f"[{_c}]", f"[{_nm}({_c})]")

        zscore_fields = {a['field'] for a in alerts}
        if not alerts or len(alerts) < 2:
            kpi = _build_alert_kpi(row)
            fixed_alerts = adapter.evaluate_platform_alerts(kpi)
            for fa in fixed_alerts:
                field_hint = {
                    'DauPlunge': 'dau_chg', 'DauSurge': 'dau_chg',
                    'DeepRatePlunge': 'deep_rate_diff',
                    'NewUserPlunge': 'new_chg',
                    'ChurnRateRise': 'churn_rate_diff',
                    'HabitRateAchieved': 'habit_rate', 'HabitRateLow': 'habit_rate',
                }
                rid = fa.get('rule_id', '').replace('raas:Alert_', '')
                if field_hint.get(rid) not in zscore_fields:
                    fa['source'] = 'fixed_rule'
                    alerts.append(fa)

        exclude = {'T00', 'F00', 'L00', 'G00', 'P00', 'L04'}
        prog_snap = {}
        for code, date_rows in timeline_snap.items():
            if code in exclude:
                continue
            r = date_rows.get(latest_dt, {})
            prog_snap[code] = {
                'dau':        _i2(r.get('dau')),
                'churn_rate': _fn2(r.get('churn_rate')),
                'dau_chg':    _fn2(r.get('dau_chg')),
            }
        risk_progs = adapter.find_at_risk_programs(prog_snap)
        if risk_progs:
            names = [
                _pgm_name(r.get('code'),
                                row=(timeline_snap.get(r.get('code'), {}) or {}).get(latest_dt, {}))
                for r in risk_progs[:3]
            ]
            alerts.append({
                'level': 'yellow',
                'msg':   f"🟡 위험 프로그램 감지: {', '.join(names)}",
                'rule_id': 'AtRiskProgramDetected',
            })
    except Exception:
        pass

    if not alerts:
        alerts = [{'level': 'green', 'msg': '🟢 이상 없음 — 모든 지표 정상 범위', 'rule_id': 'NoAlert'}]
    return {'alerts': alerts}

def collect_anomalies(timeline: dict) -> dict:
    """전사(T00) 최신일 기준 이상탐지 alerts. 서버 get_cached_anomalies가 5분 캐시로 감쌈."""
    available = _get_available_dates(timeline)
    if not available:
        return {'alerts': []}
    latest_dt = available[-1]
    row = timeline.get('T00', {}).get(latest_dt, {})
    return _evaluate_alerts(row, timeline, latest_dt)


# ─── CLI 자가 테스트 ─────────────────────────────────────────────────────
