"""통합 시계열 접근 계층 (A-lite) — raas_series.

레지스트리에 선언된 소스를 물리 형태(shape)와 무관하게 하나의 접근자로 조회한다.
모든 지표를 [(date, value)] 시계열 또는 표준 셀(dict)로 **균일하게** 돌려주므로,
교차지표 연산(Phase 2)이 소스를 몰라도 지표끼리 정렬·비교·상관할 수 있다.

원칙: 여기 있는 건 **구조 어댑터**(shape → 표준형)뿐 — 도메인 규칙·판정은 없다(온톨로지·LLM 몫).
날짜 형식은 소스 공통 'YYYY/MM/DD'. 값 코어션은 datasource._fn 재사용.
"""
import raas_datasource as DSRC
import raas_metrics_registry as REG

_fn = DSRC._fn
_GRID_SHAPES = {"flat", "profile"}   # 접근자 대상 — {code:{date:...}} 격자 소스만


def _grid(src):
    return bool(src) and src.shape in _GRID_SHAPES


def _index(src):
    fn = getattr(DSRC, (src.index_fn or "").split("(")[0], None)
    idx = fn() if callable(fn) else None
    return idx if isinstance(idx, dict) else {}


def _default_window(src):
    return (src.available_periods or ["1D"])[0]


def _leaf(src, rec, window, dims):
    """소스 형태에 맞춰 (code,date) 레코드에서 표준 셀(dict)을 뽑는다.
       flat → 행 dict 그대로 · profile → 지정 창·깊이의 {카테고리:값}."""
    if rec is None:
        return None
    if src.shape == "flat":
        return rec
    if src.shape == "profile":
        w = window or _default_window(src)
        depth = (dims or {}).get("DEPTH", "ALL")
        return (rec.get(w) or {}).get(depth)
    return None


# ── 공개 접근자 ──────────────────────────────────────────────
def cell(source, code, date, window=None, dims=None) -> dict:
    """(source, code, date)의 표준 셀 dict. flat=행, profile=해당 창·깊이의 {카테고리:값}."""
    src = REG.get_source(source)
    if not _grid(src):
        return {}
    idx = _index(src)
    rec = (idx.get((code or "").upper()) or {}).get((date or "").replace("-", "/"))
    return _leaf(src, rec, window, dims) or {}


def available_dates(source, code, window=None, dims=None) -> list:
    """해당 창·깊이에서 값이 있는 날짜(오름차순)."""
    src = REG.get_source(source)
    if not _grid(src):
        return []
    idx = _index(src)
    out = [d for d, rec in (idx.get((code or "").upper()) or {}).items()
           if _leaf(src, rec, window, dims)]
    return sorted(out)


def series(source, code, field, window=None, dims=None) -> list:
    """[(date, float)] 오름차순 — 소스 형태 무관 단일 지표 시계열."""
    src = REG.get_source(source)
    if not _grid(src):
        return []
    idx = _index(src)
    out = []
    for d, rec in (idx.get((code or "").upper()) or {}).items():
        leaf = _leaf(src, rec, window, dims)
        if leaf is None:
            continue
        v = _fn(leaf.get(field))
        if v is not None:
            out.append((d, v))
    return sorted(out)


def snapshot(source, code, field, date=None, window=None, dims=None):
    """단일 값 — date 지정 시 그 날, 없으면 최신 관측치."""
    s = series(source, code, field, window, dims)
    if not s:
        return None
    if date:
        nd = date.replace("-", "/")
        return next((v for d, v in s if d == nd), None)
    return s[-1][1]


def period_avg(source, code, field, days, window=None, dims=None):
    """최근 days개 관측치 평균(저장 평균 없음 — 시계열에서 파생). days=0이면 전체."""
    s = series(source, code, field, window, dims)
    tail = [v for _, v in (s[-days:] if days else s)]
    return round(sum(tail) / len(tail), 2) if tail else None


def ranking(source, field, date=None, window=None, dims=None, codes=None, days=None) -> list:
    """[(code, value)] 내림차순 — 코드별 순위.
       days 지정 시 각 코드의 최근 days개 관측치 평균으로(기간 순위),
       date 지정 시 그 날, 둘 다 없으면 각 코드 최신."""
    src = REG.get_source(source)
    if not _grid(src):
        return []
    idx = _index(src)
    nd = date.replace("-", "/") if date else None
    out = []
    for code in (codes or list(idx.keys())):
        code = code.upper()
        if days:
            v = period_avg(source, code, field, days, window, dims)
        elif nd:
            leaf = _leaf(src, (idx.get(code) or {}).get(nd), window, dims)
            v = _fn((leaf or {}).get(field))
        else:
            v = snapshot(source, code, field, None, window, dims)
        if v is not None:
            out.append((code, v))
    return sorted(out, key=lambda x: x[1], reverse=True)
