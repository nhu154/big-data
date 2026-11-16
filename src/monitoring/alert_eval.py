"""Danh gia canh bao: dien bien cua gia 7 va 30 ngay sau moi ngay bat canh bao.
Chay tu thu muc goc cua repo: python src/monitoring/alert_eval.py (can scipy)"""
import os; os.makedirs("reports", exist_ok=True)
import pandas as pd, numpy as np
from scipy import stats
d = pd.read_csv('outputs/btc_daily.csv', parse_dates=['Date'])
d = d[d.Record_Count >= 1440].sort_values('Date').reset_index(drop=True)
r = d.Daily_Return_Pct
d['Dinh'] = d.High.shift(1).rolling(90).max(); d['Day'] = d.Low.shift(1).rolling(90).min()
d['Z'] = r / r.shift(1).rolling(30).std()
d['DD'] = (d.Close / d.Close.rolling(90, min_periods=30).max() - 1) * 100
A = {'Vượt đỉnh 90 ngày': d.Close > d.Dinh, 'Rớt đáy 90 ngày': d.Close < d.Day,
     'Biến động bất thường |Z|>3': d.Z.abs() > 3, 'Giảm sâu ≥30%': d.DD <= -30}
for h in (7, 30):
    d[f'f{h}'] = (d.Close.shift(-h) / d.Close - 1) * 100
# bien dong thuc te 7 ngay toi: do lech chuan loi suat ngay t+1..t+7
d['vol7'] = pd.concat([r.shift(-k) for k in range(1, 8)], axis=1).std(axis=1)
valid = d.Dinh.notna() & d.f30.notna()
rows = []
def summ(name, m):
    s = d[m & valid]
    out = {'Loai': name, 'So ngay': len(s)}
    for h in (7, 30):
        out[f'TB {h}d'] = s[f'f{h}'].mean(); out[f'Trung vi {h}d'] = s[f'f{h}'].median(); out[f'% tang {h}d'] = (s[f'f{h}'] > 0).mean() * 100
    out['Bien dong 7d'] = s.vol7.mean()
    return out, s
base, B = summ('Mọi ngày', pd.Series(True, index=d.index))
rows.append(base)
for k, m in A.items():
    o, s = summ(k, m)
    # Mann-Whitney so voi ngay khong canh bao
    other = d[valid & ~m]
    o['p 30d'] = stats.mannwhitneyu(s.f30, other.f30).pvalue
    o['p vol'] = stats.mannwhitneyu(s.vol7.dropna(), other.vol7.dropna()).pvalue
    rows.append(o)
t = pd.DataFrame(rows); pd.set_option('display.width', 250); print(t.round(3).to_string())
# giai doan gan (test period tu 17/11/2023)
print('\n--- tu 17/11/2023 ---')
rows = []
for k, m in [('Mọi ngày', pd.Series(True, index=d.index))] + list(A.items()):
    s = d[m & valid & (d.Date >= '2023-11-17')]
    rows.append({'Loai': k, 'n': len(s), 'TB7': s.f7.mean(), 'TV7': s.f7.median(), 'tang7': (s.f7>0).mean()*100, 'TB30': s.f30.mean(), 'TV30': s.f30.median(), 'tang30': (s.f30>0).mean()*100, 'vol7': s.vol7.mean()})
print(pd.DataFrame(rows).round(2).to_string())
d.to_csv('reports/alert_eval_days.csv', index=False)
