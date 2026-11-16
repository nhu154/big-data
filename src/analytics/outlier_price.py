"""Phan tich ngoai lai cua GIA (qua loi suat ngay) tu outputs/btc_daily.csv.
Chay tu thu muc goc cua repo:  python src/analytics/outlier_price.py
Ket qua: outputs/outlier_returns.csv, outputs/outlier_returns.png
"""
import numpy as np, pandas as pd
from scipy import stats
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt

plt.rcParams["font.family"] = "DejaVu Sans"
d = pd.read_csv("outputs/btc_daily.csv", parse_dates=["Date"]).sort_values("Date")
d = d[d["Record_Count"] >= 1440].reset_index(drop=True)          # chi ngay du 1.440 phut
r = d["Daily_Return_Pct"].dropna()

# 1) IQR x 3 tren GIA tuyet doi -> khong phat hien duoc gi vi gia co xu huong tang
c = d["Close"]; cq1, cq3 = c.quantile([.25, .75])
print(f"IQR x3 tren Close: nguong tren {cq3 + 3*(cq3-cq1):,.0f} USD -> {(c > cq3 + 3*(cq3-cq1)).sum()} ngoai lai")

# 2) IQR x 3 tren LOI SUAT ngay (dung)
q1, q3 = r.quantile([.25, .75]); iqr = q3 - q1; lo, hi = q1 - 3*iqr, q3 + 3*iqr
d["outlier"] = (d["Daily_Return_Pct"] < lo) | (d["Daily_Return_Pct"] > hi)
o = d[d["outlier"]]
print(f"Loi suat: Q1={q1:.3f}% Q3={q3:.3f}% IQR={iqr:.3f} -> khoang hop le [{lo:.2f}% ; {hi:.2f}%]")
print(f"So ngay ngoai lai: {len(o)} ({len(o)/len(r):.2%}), giam {int((o.Daily_Return_Pct<lo).sum())}, tang {int((o.Daily_Return_Pct>hi).sum())}")
print("Theo nam:", o.groupby(o.Date.dt.year).size().to_dict())

# 3) duoi day: so sanh voi phan phoi chuan
z = (r - r.mean()) / r.std()
print(f"Do nhon du (excess kurtosis) = {stats.kurtosis(r):.2f}; |z|>4: {(z.abs()>4).sum()} ngay, "
      f"phan phoi chuan ky vong {len(r)*2*stats.norm.sf(4):.2f} ngay")
o[["Date", "Close", "Daily_Return_Pct"]].to_csv("outputs/outlier_returns.csv", index=False)

# 4) bieu do
fig, ax = plt.subplots(1, 2, figsize=(13, 4.6), gridspec_kw={"width_ratios": [1, 1.6]})
x = np.linspace(-50, 45, 600)
ax[0].hist(r, bins=180, density=True, color="#9ecae1", edgecolor="none", label="Lợi suất ngày thực tế")
ax[0].plot(x, stats.norm.pdf(x, r.mean(), r.std()), color="black", lw=1.2, label="Phân phối chuẩn cùng μ, σ")
for b in (lo, hi): ax[0].axvline(b, color="#d62728", ls="--", lw=1)
ax[0].set_yscale("log"); ax[0].set_ylim(1e-5, 1); ax[0].set_xlim(-50, 45)
ax[0].set_xlabel("Lợi suất ngày (%)"); ax[0].set_ylabel("Mật độ (thang log)")
ax[0].set_title("(a) Phân phối lợi suất ngày và ngưỡng IQR × 3", fontsize=10); ax[0].legend(fontsize=8, loc="upper left")
ax[1].plot(d["Date"], d["Daily_Return_Pct"], color="#9e9e9e", lw=0.5)
ax[1].scatter(o["Date"], o["Daily_Return_Pct"], s=10, color="#d62728", zorder=3, label=f"Ngoại lai ({len(o)} ngày)")
for b in (lo, hi): ax[1].axhline(b, color="#d62728", ls="--", lw=0.8)
for dt, txt in [("2013-04-11", "04/2013"), ("2020-03-12", "COVID-19\n03/2020"), ("2022-11-09", "FTX\n11/2022"), ("2013-11-18", "11/2013")]:
    row = d[d.Date == dt].iloc[0]
    ax[1].annotate(txt, (row.Date, row.Daily_Return_Pct), xytext=(8, -2 if row.Daily_Return_Pct < 0 else 2),
                   textcoords="offset points", fontsize=7.5, va="center")
ax[1].set_ylabel("Lợi suất ngày (%)"); ax[1].set_title("(b) Lợi suất ngày theo thời gian, ngoại lai đánh dấu đỏ", fontsize=10)
ax[1].legend(fontsize=8, loc="lower right"); ax[1].grid(alpha=.3)
fig.tight_layout(); fig.savefig("outputs/outlier_returns.png", dpi=200)
print("Da luu outputs/outlier_returns.png")
