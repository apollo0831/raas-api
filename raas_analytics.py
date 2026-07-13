"""교차지표 연산 (결정적) — raas_analytics.

통합 접근자(raas_series)가 주는 [(date, value)] 시계열들을 공통 날짜로 정렬해
동반움직임·상관·분해를 낸다. **숫자만** — "무엇이 무엇과 관련되는가"의 판단(인과·의미)은
온톨로지·LLM 몫이다(여긴 상관계수·변화량까지). Phase 2 코어.
"""
import math


def align(a, b):
    """두 [(date,val)] 시계열을 공통 날짜로 정렬 → ([va…], [vb…], [date…])."""
    da, db = dict(a), dict(b)
    dates = sorted(set(da) & set(db))
    return [da[d] for d in dates], [db[d] for d in dates], dates


def pearson(xs, ys):
    """피어슨 상관계수 — 관측치 3개 미만이거나 분산 0이면 None."""
    n = len(xs)
    if n < 3:
        return None
    mx, my = sum(xs) / n, sum(ys) / n
    cov = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    vx = sum((x - mx) ** 2 for x in xs)
    vy = sum((y - my) ** 2 for y in ys)
    if vx <= 0 or vy <= 0:
        return None
    return cov / math.sqrt(vx * vy)


def change(series):
    """시계열 처음→끝 변화(절대·%). 관측치 2개 미만이면 None."""
    if len(series) < 2:
        return None
    a, b = series[0][1], series[-1][1]
    return {"from": round(a, 2), "to": round(b, 2), "abs": round(b - a, 2),
            "pct": (round((b - a) / a * 100, 1) if a else None)}


def co_movement(a, b, min_n=5):
    """두 시계열의 동반움직임 — 공통창에서 r·방향·표본수."""
    xs, ys, dates = align(a, b)
    if len(dates) < min_n:
        return None
    r = pearson(xs, ys)
    if r is None:
        return None
    return {"r": round(r, 3), "n": len(dates), "window": [dates[0], dates[-1]],
            "direction": "동행(+)" if r > 0 else "역행(-)",
            "strength": _strength(abs(r))}


def _strength(ar):
    return ("강" if ar >= 0.7 else "중" if ar >= 0.4 else "약" if ar >= 0.2 else "미미")


def correlate_many(target, factors, min_n=5):
    """target [(date,val)] 대비 factors {name: series} 상관 — |r| 내림차순."""
    out = []
    for name, ser in factors.items():
        cm = co_movement(target, ser, min_n=min_n)
        if cm:
            out.append({"factor": name, **cm})
    return sorted(out, key=lambda x: abs(x["r"]), reverse=True)


def decompose(target, factors, min_n=5):
    """target 변화와, 각 factor의 '같은 창에서의 변화 + target과의 상관'을 요약.
       인과가 아니라 '무엇이 함께 움직였나'의 결정적 재료(해석은 상위 계층)."""
    rows = []
    for name, ser in factors.items():
        xs, ys, dates = align(target, ser)
        if len(dates) < min_n:
            continue
        r = pearson(xs, ys)
        if r is None:
            continue
        rows.append({"factor": name, "r": round(r, 3), "n": len(dates),
                     "strength": _strength(abs(r)),
                     "direction": "동행(+)" if r > 0 else "역행(-)",
                     "change": change(list(zip(dates, ys)))})
    rows.sort(key=lambda x: abs(x["r"]), reverse=True)
    # target 변화는 factor들과 겹치는 공통창으로 맞춰 보고(모든 factor가 같은 창은 아니므로 target 자체 창)
    return {"target_change": change(target), "factors": rows}


def rank_by_change(code_series, ascending=True):
    """{code: [(date,val)]} → 시작→끝 변화율(%) 순위. ascending=True면 하락 상위."""
    rows = []
    for code, ser in code_series.items():
        ch = change(ser)
        if ch and ch["pct"] is not None:
            rows.append({"code": code, **ch})
    return sorted(rows, key=lambda x: x["pct"], reverse=not ascending)
