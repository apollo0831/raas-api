"""실시간 통합 접근자(raas_rt_series) 스모크 — 접근자 값이 현 인라인/원천과 일치하는지 대조.

RT-1a: 필드 템플릿 조립·오늘/과거일 fetch·해상도·채널 게이팅이 올바른지(LLM 호출 0).
소비자 전환(RT-1b) 전이라 '값 동등성'을 고정 — 리팩터 회귀 방지.
"""
import io, sys, os
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import raas_datasource as DSRC
import raas_rt_series as RT

ok = fail = 0
def check(name, cond, extra=""):
    global ok, fail
    if cond: ok += 1; print(f"  PASS {name}")
    else: fail += 1; print(f"  FAIL {name}  {extra}")


def _hhmm(r):
    t = str(r.get("_time") or ""); return t[11:16] if len(t) >= 16 else ""
def _num(v):
    try: return None if v in (None, "", "None") else int(float(v))
    except Exception: return None


print("── 오늘: 접근자 == 원천 필드 직독 ──")
today = DSRC.get_rt_concurrent()
if today:
    # concurrent F00
    a = RT.rt_series("concurrent", "F00", "today", 1)
    b = [( _hhmm(r), _num(r.get("F00")) ) for r in today if _hhmm(r) and _num(r.get("F00")) is not None]
    check("concurrent F00 시계열 일치", a == b, f"{len(a)} vs {len(b)}")
    # device SP × F00 → DV_SP_F00
    d = RT.rt_series("device", "F00", "today", 1, dims={"device": "SP"})
    d2 = [(_hhmm(r), _num(r.get("DV_SP_F00"))) for r in today if _hhmm(r) and _num(r.get("DV_SP_F00")) is not None]
    check("device SP×F00 → DV_SP_F00 일치", d == d2, f"{len(d)} vs {len(d2)}")
    # device 전체 → DV_SP (접미 없음)
    dt = RT.rt_series("device", "T00", "today", 1, dims={"device": "SP"})
    dt2 = [(_hhmm(r), _num(r.get("DV_SP"))) for r in today if _hhmm(r) and _num(r.get("DV_SP")) is not None]
    check("device SP×전체 → DV_SP 일치", dt == dt2)
    # sex_ratio F00 F → R_F00_SEX_F (비율 float)
    sx = RT.rt_series("sex_ratio", "F00", "today", 1, dims={"sex": "F"})
    check("sex_ratio F00 여성 → R_F00_SEX_F 존재", len(sx) > 0 and isinstance(sx[-1][1], float))
    check("sex_ratio 필드조립", RT._field(RT.RT_METRICS["sex_ratio"], "F00", {"sex": "F"}) == "R_F00_SEX_F")
    check("age_ratio 필드조립", RT._field(RT.RT_METRICS["age_ratio"], "L00", {"age": "40_44"}) == "R_L00_AGE_40_44")
    # 해상도 10분: 접근자 == 10분 그리드 직독
    a10 = RT.rt_series("concurrent", "F00", "today", 10)
    check("해상도 10분 = 분끝 0만", all(int(t[3:5]) % 10 == 0 for t, _ in a10))
    check("10분이 1분의 부분집합", set(t for t, _ in a10) <= set(t for t, _ in a))
    # window(편성창) 필터
    win = RT.rt_series("concurrent", "F00", "today", 1, window=("00:00", "00:10"))
    check("window 필터 [00:00,00:10)", all("00:00" <= t < "00:10" for t, _ in win))
else:
    print("  (오늘 실시간 없음 — 건너뜀)")

print("── 채널 게이팅 ──")
check("sex_ratio는 인증채널(F00/L00)만", "G00" not in RT.metric_channels("sex_ratio")
      and RT.rt_series("sex_ratio", "G00", "today", dims={"sex": "F"}) == [])
check("inflow에 G00 있음", "G00" in RT.metric_channels("inflow"))
check("미등록 지표 → 빈", RT.rt_series("nope", "F00") == [])

print("── rt_table(다열) ──")
tb = RT.rt_table("concurrent", when="today", resolution=10)
check("table 헤더=시각+전채널", tb["header"][0] == "시각" and "파워FM" in tb["header"])
check("table 행 열수 = 채널수+1", (not tb["rows"]) or len(tb["rows"][0]) == len(tb["channels"]) + 1)

print("── 과거일 광역 fetch (rt_fetch date) ==  get_rt_history 채널부분 ──")
earliest = DSRC.get_rt_earliest()
print(f"  보관 시작일: {earliest}")
if earliest:
    date = earliest                       # 보관 시작일(존재 보장)
    hist = DSRC.get_rt_history(date)      # 기존 narrow(채널만)
    wide = DSRC.rt_fetch("rt_concurrent", date)  # 신규 광역
    hv = [(_hhmm(r), _num(r.get("F00"))) for r in hist if _hhmm(r)]
    wv = [(_hhmm(r), _num(r.get("F00"))) for r in wide if _hhmm(r)]
    check("과거일 광역 F00 == narrow F00 (동일 원천)", hv == wv, f"{len(hv)} vs {len(wv)}")
    check("과거일 광역에 디바이스·성별 필드 존재(narrow엔 없음)",
          bool(wide) and ("DV_SP" in wide[-1]) and ("R_F00_SEX_F" in wide[-1]))
    rday = RT.rt_series("concurrent", "F00", date, 10)
    check("접근자 과거일 concurrent F00(10분)", all(int(t[3:5]) % 10 == 0 for t, _ in rday))
    check("과거 범위밖 → rt_available ok=False", not RT.rt_available("2020-01-01")["ok"])

print(f"\n{'✓ 전체 통과' if not fail else f'✗ {fail}건 실패'} (ok={ok}, fail={fail})")
sys.exit(1 if fail else 0)
