"""ZScoreDetector 동작 검증 — 실제 timeline 데이터 사용."""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))

from raas_datasource import load_timeline as _load_timeline, get_available_dates

def _csv_fallback(_query):
    raise RuntimeError("no splunk — use CSV fallback")

print("=== ZScoreDetector 동작 검증 ===\n")

# 1. timeline 로드
print("[1] Timeline 로드...")
timeline, source = _load_timeline(_csv_fallback)
dates = get_available_dates(timeline)
print(f"    source={source}, codes={len(timeline)}, dates={len(dates)}")
ref_date = dates[-1]
print(f"    평가 기준일: {ref_date}\n")

# 2. ZScoreDetector 직접 실행
print("[2] ZScoreDetector.evaluate() 실행...")
from raas_onto import get_detector
detector = get_detector()
alerts = detector.evaluate(timeline, ref_date)
print(f"    감지 알림 수: {len(alerts)}")
print()

# 등급별 분류
for level in ('red', 'yellow', 'green'):
    lvl_alerts = [a for a in alerts if a['level'] == level]
    if not lvl_alerts:
        continue
    emoji = {'red': '🔴', 'yellow': '🟡', 'green': '🟢'}[level]
    print(f"  {emoji} {level.upper()} ({len(lvl_alerts)}건)")
    for a in lvl_alerts[:5]:
        print(f"    [{a['code']:4s}] {a['label']:<16} val={a['value']:+.2f}  z={a['z']:+.2f}  base={a['baseline_median']:+.2f}  n={a['baseline_n']}")
    print()

# 3. 어댑터 연동 확인
print("[3] adapter.evaluate_zscore_alerts() 실행...")
from raas_onto import get_adapter
adapter = get_adapter()
fmt_alerts = adapter.evaluate_zscore_alerts(timeline, ref_date)
print(f"    포맷된 알림: {len(fmt_alerts)}건")
for a in fmt_alerts[:5]:
    print(f"    {a['msg']}")
print()

# 4. calibration_report (최근 30일)
print("[4] calibration_report() 실행 (최근 30일)...")
if len(dates) >= 30:
    report = detector.generate_calibration_report(timeline, dates[-30], dates[-1])
    print(f"    기간: {report['period']}")
    print(f"    총 평가일: {report['total_days']}일")
    print(f"    YELLOW/일: {report['yellow_rate']:.1f}건  RED/일: {report['red_rate']:.1f}건")
    print(f"    현재 파라미터: {report['params']}")
else:
    print(f"    (데이터 부족 — {len(dates)}일치만 있음)")

print("\n=== 검증 완료 ===")
