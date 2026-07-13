"""통합 접근자(raas_series) 스모크 — 기존 소스별 헬퍼와 값이 일치하는지 대조.

접근자가 소스 형태(flat/profile)를 올바로 어댑트하는지, 균일 반환이 기존 경로와
같은 숫자를 내는지 확인한다(LLM 호출 0). 실패 시 리팩터 회귀.
"""
import io, sys, os
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import raas_datasource as DSRC
import raas_series as SR

ok = fail = 0
def check(name, cond, extra=""):
    global ok, fail
    if cond:
        ok += 1; print(f"  PASS {name}")
    else:
        fail += 1; print(f"  FAIL {name}  {extra}")


def _first_code(idx):
    return next(iter(idx), None)


print("── engagement: series == get_engagement_series ──")
eidx = DSRC.get_engagement_index()
ec = _first_code(eidx)
if ec:
    a = SR.series("engagement", ec, "SMS")
    b = DSRC.get_engagement_series(ec, "SMS")
    check("engagement SMS 시계열 일치", a == b, f"len {len(a)} vs {len(b)}")
    check("engagement snapshot=최신", (not a) or SR.snapshot("engagement", ec, "SMS") == a[-1][1])
else:
    print("  (engagement 데이터 없음 — 건너뜀)")

print("── history: series == get_history_series ──")
hidx = DSRC.get_history_index()
hc = _first_code(hidx)
if hc:
    a = SR.series("history_archive", hc, "dau")
    b = DSRC.get_history_series(hc, "dau")
    check("history dau 시계열 일치", a == b, f"len {len(a)} vs {len(b)}")

print("── kpi: series == get_metric_trend(timeline) ──")
tl = DSRC.get_timeline()
kc = _first_code(tl)
if kc:
    a = SR.series("kpi", kc, "dau")
    trend = [(d, v) for d, v in DSRC.get_metric_trend(tl, kc, "dau", days=10**6) if v is not None]
    check("kpi dau 시계열 일치", a == sorted(trend), f"len {len(a)} vs {len(trend)}")
    rk = SR.ranking("kpi", "dau")
    check("kpi dau 랭킹 내림차순", all(rk[i][1] >= rk[i+1][1] for i in range(len(rk)-1)))
    check("kpi 랭킹 비어있지 않음", len(rk) > 0)

print("── profile: cell/series 어댑트(창·깊이) ──")
gidx = DSRC.get_program_gender_index()
gc = _first_code(gidx)
if gc:
    dates = SR.available_dates("pgm_gender", gc, window="1D", dims={"DEPTH": "ALL"})
    check("gender 1D/ALL 날짜 존재", len(dates) > 0)
    c = SR.cell("pgm_gender", gc, dates[-1], window="1D", dims={"DEPTH": "ALL"})
    check("gender 셀에 F/M", "F" in c and "M" in c, str(c))
    tot = sum(float(v) for v in c.values() if str(v).replace(".", "", 1).isdigit())
    check("gender 셀 합 ≈100", 95 <= tot <= 105, f"합={tot}")
    fser = SR.series("pgm_gender", gc, "F", window="1D", dims={"DEPTH": "ALL"})
    check("gender F 시계열", len(fser) == len(dates))
    # 주간 창(1W) 접근 — 로더 통합 확인
    wdates = SR.available_dates("pgm_gender", gc, window="1W", dims={"DEPTH": "ALL"})
    check("gender 1W 창 접근 가능", len(wdates) > 0, f"1W 날짜수={len(wdates)}")

print("── device: 1D만·AI 병합 셀 ──")
didx = DSRC.get_program_device_index()
dc = _first_code(didx)
if dc:
    ddates = SR.available_dates("pgm_device", dc, dims={"DEPTH": "ALL"})
    c = SR.cell("pgm_device", dc, ddates[-1], dims={"DEPTH": "ALL"})
    check("device 셀에 AI 병합키", "AI" in c, str(list(c)))
    check("device 1W 미보유→빈 날짜", SR.available_dates("pgm_device", dc, window="1W") == [])

print("── 미등록/비대상 소스 안전 ──")
check("unknown 소스 → 빈", SR.series("nope", "F00", "x") == [] and SR.cell("nope", "F00", "2026/01/01") == {})
check("editorial 소스(shape='') → 빈", SR.series("today_lineup", "F00", "x") == [])

print(f"\n{'✓ 전체 통과' if not fail else f'✗ {fail}건 실패'} (ok={ok}, fail={fail})")
sys.exit(1 if fail else 0)
