import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')

paths = ['data/raas_kpi_latest.csv', 'raas_kpi_latest.csv']
df = None
for p in paths:
    try:
        df = pd.read_csv(p, encoding='utf-8-sig')
        print(f'Loaded: {p}, shape={df.shape}')
        break
    except Exception as e:
        pass

if df is None:
    print('ERROR: CSV not found')
    exit()

t00 = df[df['PGM_CODE'] == 'T00'].copy()
print(f'T00 rows: {len(t00)}, dates: {t00["DATE"].min()} ~ {t00["DATE"].max()}')
print()

metrics = [
    ('dau_chg',         'DAU WoW(%)'),
    ('dau_r7_chg',      '7일롤링 WoW(%)'),
    ('deep_rate',       '깊은청취율(%)'),
    ('deep_rate_diff',  '깊은청취율 Δpp'),
    ('real_rate',       '실청취율(%)'),
    ('real_rate_diff',  '실청취율 Δpp'),
    ('engage_rate',     '참여율(%)'),
    ('new_share',       '신규비중(%)'),
    ('new_chg',         '신규 WoW(%)'),
    ('react_rate',      '복귀율(%)'),
    ('react_rate_diff', '복귀율 Δpp'),
    ('churn_rate',      '이탈률(%)'),
    ('churn_rate_diff', '이탈률 Δpp'),
    ('habit_rate',      '습관형성률(%)'),
    ('habit_rate_diff', '습관형성률 Δpp'),
    ('d1_ret',          'D1 유지율(%)'),
    ('d7_ret',          'D7 유지율(%)'),
    ('w1_ret',          'W1 유지율(%)'),
    ('m1_ret',          'M1 유지율(%)'),
    ('new_d1_ret',      '신규D1 유지율(%)'),
    ('new_d7_ret',      '신규D7 유지율(%)'),
    ('new_w1_ret',      '신규W1 유지율(%)'),
    ('new_m1_ret',      '신규M1 유지율(%)'),
]

hdr = f"{'지표':<22} {'N':>3} {'min':>7} {'p10':>7} {'p25':>7} {'median':>7} {'p75':>7} {'p90':>7} {'max':>7} {'mean':>7} {'std':>6}"
print(hdr)
print('-' * len(hdr))

for col, label in metrics:
    if col not in t00.columns:
        print(f'{label:<22} NOT FOUND')
        continue
    s = pd.to_numeric(t00[col], errors='coerce').dropna()
    if len(s) == 0:
        print(f'{label:<22} NO DATA')
        continue
    print(f'{label:<22} {len(s):>3} {s.min():>7.1f} {s.quantile(0.1):>7.1f} {s.quantile(0.25):>7.1f} {s.median():>7.1f} {s.quantile(0.75):>7.1f} {s.quantile(0.9):>7.1f} {s.max():>7.1f} {s.mean():>7.1f} {s.std():>6.1f}')
