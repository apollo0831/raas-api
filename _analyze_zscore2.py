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
df['weekday'] = df['DATE'].dt.weekday
df['dau_chg'] = pd.to_numeric(df['dau_chg'], errors='coerce')
df['dau'] = pd.to_numeric(df['dau'], errors='coerce')

t00 = df[df['PGM_CODE'] == 'T00'].copy()
wd_names = ['Mon','Tue','Wed','Thu','Fri','Sat','Sun']

# 4. 정규성 검사
print("=== 4. T00 skew/kurt ===")
metrics_check = [
    ('dau_chg', 'DAU_WoW'),
    ('deep_rate_diff', 'DeepRate_delta'),
    ('new_chg', 'New_WoW'),
    ('churn_rate_diff', 'ChurnRate_delta'),
    ('habit_rate', 'HabitRate_abs'),
    ('react_rate', 'ReactRate_abs'),
    ('new_d1_ret', 'NewD1_abs'),
]
print(f"{'metric':<20} {'N':>4} {'skew':>7} {'kurt':>7} {'note'}")
for col, label in metrics_check:
    if col not in t00.columns:
        continue
    s = pd.to_numeric(t00[col], errors='coerce').dropna()
    if len(s) < 5:
        continue
    skew = s.skew()
    kurt = s.kurtosis()
    ok = "OK" if abs(skew) < 1.0 and abs(kurt) < 3.0 else ("high_skew" if abs(skew) >= 1.0 else "high_kurt")
    print(f"{label:<20} {len(s):>4} {skew:>7.2f} {kurt:>7.2f}  {ok}")

# 5. 샘플 수
print("\n=== 5. sample counts ===")
ch_codes = ['T00', 'F00', 'L00', 'G00', 'P00', 'L04']
pgm_df = df[~df['PGM_CODE'].isin(ch_codes)].copy()
latest = pgm_df.groupby('PGM_CODE')['dau'].median().reset_index()
large_pgm = latest[latest['dau'] >= 10000]['PGM_CODE'].tolist()
mid_pgm   = latest[(latest['dau'] >= 1000) & (latest['dau'] < 10000)]['PGM_CODE'].tolist()
print(f"T00 total days: {len(t00)}, per-weekday avg: {len(t00)/7:.1f}")
print(f"Large pgm count: {len(large_pgm)}, Mid pgm count: {len(mid_pgm)}")
print(f"Large pgm rows total: {len(pgm_df[pgm_df['PGM_CODE'].isin(large_pgm)])}")
print(f"Per large_pgm per weekday: {len(pgm_df[pgm_df['PGM_CODE'].isin(large_pgm)]) / max(len(large_pgm),1) / 7:.1f}")

# 6. rolling 30d z-score simulation (T00 dau_chg)
print("\n=== 6. Rolling 30d z-score simulation (T00 dau_chg) ===")
t00_s = t00.sort_values('DATE').reset_index(drop=True)
t00_s['dau_chg'] = pd.to_numeric(t00_s['dau_chg'], errors='coerce')
zs = []
for i in range(len(t00_s)):
    window = t00_s.loc[max(0, i-30):i-1, 'dau_chg'].dropna()
    if len(window) < 14:
        zs.append(None)
        continue
    mu, sigma = window.mean(), window.std()
    z = (t00_s.loc[i, 'dau_chg'] - mu) / (sigma + 1e-9)
    zs.append(round(z, 2))
t00_s['z'] = zs
valid = t00_s[t00_s['z'].notna()]
print(f"z computable days: {len(valid)} / {len(t00_s)}")
print(f"|z|>=2.0: {(valid['z'].abs()>=2.0).sum()} days  (~{(valid['z'].abs()>=2.0).mean()*100:.0f}% of valid)")
print(f"|z|>=3.0: {(valid['z'].abs()>=3.0).sum()} days  (~{(valid['z'].abs()>=3.0).mean()*100:.0f}% of valid)")
print()
print("Triggered alerts:")
for _, row in valid[valid['z'].abs() >= 2.0].iterrows():
    lvl = "RED " if abs(row['z']) >= 3.0 else "YLW "
    print(f"  {row['DATE'].strftime('%m/%d')} ({wd_names[row['weekday']]}) dau_chg={row['dau_chg']:+.1f}% z={row['z']:+.2f} {lvl}")

# 7. weekday z-score (same-weekday baseline)
print("\n=== 7. Same-weekday z-score simulation (T00 dau_chg) ===")
zs_wd = []
for i, row in t00_s.iterrows():
    wd = row['weekday']
    prior = t00_s.loc[:i-1]
    same_wd = prior[prior['weekday'] == wd]['dau_chg'].dropna()
    if len(same_wd) < 4:
        zs_wd.append(None)
        continue
    mu, sigma = same_wd.mean(), same_wd.std()
    z = (row['dau_chg'] - mu) / (sigma + 1e-9)
    zs_wd.append(round(z, 2))
t00_s['z_wd'] = zs_wd
valid_wd = t00_s[t00_s['z_wd'].notna()]
print(f"z_weekday computable days: {len(valid_wd)} / {len(t00_s)}")
print(f"|z|>=2.0: {(valid_wd['z_wd'].abs()>=2.0).sum()}  |z|>=3.0: {(valid_wd['z_wd'].abs()>=3.0).sum()}")
print()
print("Triggered (weekday z):")
for _, row in valid_wd[valid_wd['z_wd'].abs() >= 2.0].iterrows():
    lvl = "RED" if abs(row['z_wd']) >= 3.0 else "YLW"
    print(f"  {row['DATE'].strftime('%m/%d')} ({wd_names[row['weekday']]}) dau_chg={row['dau_chg']:+.1f}% z_wd={row['z_wd']:+.2f} {lvl}")

# 8. deep_rate_diff rolling z
print("\n=== 8. Rolling 30d z-score (T00 deep_rate_diff) ===")
t00_s['deep_rate_diff'] = pd.to_numeric(t00_s['deep_rate_diff'], errors='coerce')
zs_d = []
for i in range(len(t00_s)):
    window = t00_s.loc[max(0,i-30):i-1, 'deep_rate_diff'].dropna()
    if len(window) < 14:
        zs_d.append(None)
        continue
    mu, sigma = window.mean(), window.std()
    z = (t00_s.loc[i, 'deep_rate_diff'] - mu) / (sigma + 1e-9)
    zs_d.append(round(z, 2))
t00_s['z_deep'] = zs_d
valid_d = t00_s[t00_s['z_deep'].notna()]
print(f"|z|>=2.0: {(valid_d['z_deep'].abs()>=2.0).sum()}  |z|>=3.0: {(valid_d['z_deep'].abs()>=3.0).sum()}")
for _, row in valid_d[valid_d['z_deep'].abs() >= 2.0].iterrows():
    lvl = "RED" if abs(row['z_deep']) >= 3.0 else "YLW"
    print(f"  {row['DATE'].strftime('%m/%d')} ({wd_names[row['weekday']]}) deep_diff={row['deep_rate_diff']:+.2f}pp z={row['z_deep']:+.2f} {lvl}")
