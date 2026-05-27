"""
Z-score 기반 이상 감지 설계 검증용 분석 스크립트
- 요일별 분포, 카테고리별 분산, 지표별 정규성 확인
"""
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')

paths = ['data/raas_kpi_latest.csv', 'raas_kpi_latest.csv']
df = None
for p in paths:
    try:
        df = pd.read_csv(p, encoding='utf-8-sig')
        break
    except Exception:
        pass

df['DATE'] = pd.to_datetime(df['DATE'].str.replace('/', '-'))
df['weekday'] = df['DATE'].dt.weekday  # 0=월 6=일

# ── 1. 요일별 DAU WoW 분포 (T00) ──────────────────────────────────
print("=== 1. T00 DAU WoW(dau_chg) 요일별 분포 ===")
t00 = df[df['PGM_CODE'] == 'T00'].copy()
t00['dau_chg'] = pd.to_numeric(t00['dau_chg'], errors='coerce')
wd_names = ['월', '화', '수', '목', '금', '토', '일']
print(f"{'요일':<4} {'N':>3} {'mean':>7} {'std':>7} {'min':>7} {'max':>7}")
for wd in range(7):
    s = t00[t00['weekday'] == wd]['dau_chg'].dropna()
    if len(s) == 0: continue
    print(f"{wd_names[wd]:<4} {len(s):>3} {s.mean():>7.1f} {s.std():>7.1f} {s.min():>7.1f} {s.max():>7.1f}")

# ── 2. 카테고리별 DAU WoW 분포 ────────────────────────────────────
print("\n=== 2. 카테고리별 DAU WoW 분포 ===")
categories = {
    'T00(전체)': ['T00'],
    '채널': ['F00', 'L00', 'G00', 'P00'],
    '대형프로그램(DAU1만+)': None,
    '중형프로그램(DAU1천~1만)': None,
}
df['dau_chg'] = pd.to_numeric(df['dau_chg'], errors='coerce')
df['dau'] = pd.to_numeric(df['dau'], errors='coerce')

ch_codes = ['T00', 'F00', 'L00', 'G00', 'P00', 'L04']
pgm_df = df[~df['PGM_CODE'].isin(ch_codes)].copy()
latest = pgm_df.groupby('PGM_CODE')['dau'].median().reset_index()

large_pgm = latest[latest['dau'] >= 10000]['PGM_CODE'].tolist()
mid_pgm   = latest[(latest['dau'] >= 1000) & (latest['dau'] < 10000)]['PGM_CODE'].tolist()

groups = {
    'T00(전체)': df[df['PGM_CODE'] == 'T00']['dau_chg'],
    '채널(F00/L00/G00/P00)': df[df['PGM_CODE'].isin(['F00','L00','G00','P00'])]['dau_chg'],
    '대형프로그램(DAU 1만+)': df[df['PGM_CODE'].isin(large_pgm)]['dau_chg'],
    '중형프로그램(DAU 1천~1만)': df[df['PGM_CODE'].isin(mid_pgm)]['dau_chg'],
}
print(f"{'카테고리':<28} {'N':>4} {'mean':>7} {'std':>7} {'p10':>7} {'p90':>7} {'min':>7} {'max':>7}")
for name, s in groups.items():
    s = s.dropna()
    print(f"{name:<28} {len(s):>4} {s.mean():>7.1f} {s.std():>7.1f} {s.quantile(0.1):>7.1f} {s.quantile(0.9):>7.1f} {s.min():>7.1f} {s.max():>7.1f}")

# ── 3. 요일 보정 후 잔차의 분포 변화 ──────────────────────────────
print("\n=== 3. 요일 보정 효과 (T00 dau_chg) ===")
t00 = t00.copy()
wd_means = t00.groupby('weekday')['dau_chg'].mean()
t00['dau_chg_detrended'] = t00['dau_chg'] - t00['weekday'].map(wd_means)
print(f"보정 전: mean={t00['dau_chg'].mean():.2f}, std={t00['dau_chg'].std():.2f}")
print(f"보정 후: mean={t00['dau_chg_detrended'].mean():.2f}, std={t00['dau_chg_detrended'].std():.2f}")
print(f"요일별 평균: {dict(zip(wd_names, [round(wd_means.get(i,0),1) for i in range(7)]))}")

# ── 4. 핵심 지표 정규성 검사 (왜도/첨도) ────────────────────────────
print("\n=== 4. T00 지표별 왜도(skew)/첨도(kurt) — 정규성 판단 ===")
metrics_check = [
    ('dau_chg', 'DAU WoW'),
    ('deep_rate_diff', '깊은청취율 Δ'),
    ('new_chg', '신규 WoW'),
    ('churn_rate_diff', '이탈률 Δ'),
    ('habit_rate', '습관형성률'),
    ('react_rate', '복귀율'),
    ('new_d1_ret', '신규D1 유지율'),
]
print(f"{'지표':<18} {'N':>4} {'skew':>7} {'kurt':>7} {'정규성판단'}")
for col, label in metrics_check:
    if col not in t00.columns: continue
    s = pd.to_numeric(t00[col], errors='coerce').dropna()
    if len(s) < 5: continue
    skew = s.skew()
    kurt = s.kurtosis()
    normal = "✅ 양호" if abs(skew) < 1.0 and abs(kurt) < 3.0 else ("⚠️ 왜도 큼" if abs(skew) >= 1.0 else "⚠️ 첨도 큼")
    print(f"{label:<18} {len(s):>4} {skew:>7.2f} {kurt:>7.2f}  {normal}")

# ── 5. 프로그램별 요일 샘플 수 확인 ─────────────────────────────────
print("\n=== 5. 요일별 샘플 수 (T00 기준) ===")
print(f"전체 날짜 수: {len(t00)} → 요일당 평균 {len(t00)/7:.1f}개")
print(f"프로그램 단위: {len(pgm_df['PGM_CODE'].unique())}개 프로그램 × 요일당 ~{len(t00)/7:.0f}개 = 신뢰도 낮음")
print(f"→ 요일별 독립 baseline은 T00/채널만 가능 (프로그램은 전체 롤링 사용)")

# ── 6. 롤링 30일 z-score 시뮬레이션 (T00 dau_chg) ───────────────────
print("\n=== 6. 롤링 30일 z-score 시뮬레이션 (T00 dau_chg) ===")
t00_sorted = t00.sort_values('DATE').reset_index(drop=True)
t00_sorted['dau_chg'] = pd.to_numeric(t00_sorted['dau_chg'], errors='coerce')
results = []
for i in range(len(t00_sorted)):
    window = t00_sorted.loc[max(0, i-30):i-1, 'dau_chg'].dropna()
    if len(window) < 14:
        results.append(None)
        continue
    z = (t00_sorted.loc[i, 'dau_chg'] - window.mean()) / (window.std() + 1e-9)
    results.append(round(z, 2))
t00_sorted['z_score'] = results
triggered = t00_sorted[t00_sorted['z_score'].notna() & (t00_sorted['z_score'].abs() >= 2.0)]
print(f"z-score 계산 가능한 날: {t00_sorted['z_score'].notna().sum()}일")
print(f"|z| >= 2.0 감지 건수: {len(triggered)}건")
print(f"|z| >= 3.0 감지 건수: {len(t00_sorted[t00_sorted['z_score'].notna() & (t00_sorted['z_score'].abs() >= 3.0)])}건")
print()
if len(triggered) > 0:
    for _, row in triggered.iterrows():
        level = "🔴RED" if abs(row['z_score']) >= 3.0 else "🟡YLW"
        print(f"  {row['DATE'].strftime('%Y-%m-%d')} ({wd_names[row['weekday']]}) dau_chg={row['dau_chg']:+.1f}% z={row['z_score']:+.2f} {level}")
