"""Bieu do nen (candlestick) cho Bitcoin tu outputs/btc_monthly.csv va outputs/btc_daily.csv.
Chay tu thu muc goc cua repo:  python src/analytics/candle_chart.py
Ket qua: outputs/candle_monthly.png (toan bo lich su, nen thang, thang log)
         outputs/candle_daily_6m.png (6 thang gan nhat, nen ngay + MA7, MA30, khoi luong,
                                       danh dau ngay vuot dinh / rot day 90 ngay)
"""
import numpy as np, pandas as pd
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

plt.rcParams["font.family"] = "DejaVu Sans"
UP, DOWN = "#26a69a", "#ef5350"
N = 90                                                        # cua so dinh / day


def candles(ax, df, width):
    for _, r in df.iterrows():
        col = UP if r.Close >= r.Open else DOWN
        ax.vlines(r.x, r.Low, r.High, color=col, lw=0.8)
        ax.add_patch(plt.Rectangle((r.x - width / 2, min(r.Open, r.Close)), width,
                                   max(abs(r.Close - r.Open), 1e-9), facecolor=col, edgecolor=col, lw=0.6))


# ---------- nen thang toan bo lich su ----------
m = pd.read_csv("outputs/btc_monthly.csv")
m["x"] = mdates.date2num(pd.to_datetime(m["YearMonth"] + "-15"))
fig, ax = plt.subplots(figsize=(12, 5))
candles(ax, m, 20)
ax.set_yscale("log"); ax.xaxis_date(); ax.grid(alpha=.3, which="both", lw=.4)
ax.set_ylabel("USD (thang log)")
ax.set_title("Biểu đồ nến tháng BTC/USD, 01/2012 - 11/2026 (xanh: tháng tăng, đỏ: tháng giảm)")
fig.tight_layout(); fig.savefig("outputs/candle_monthly.png", dpi=200); plt.close(fig)

# ---------- nen ngay 6 thang gan nhat ----------
d = pd.read_csv("outputs/btc_daily.csv", parse_dates=["Date"]).sort_values("Date")
d = d[d["Record_Count"] >= 1440].reset_index(drop=True)
d["Dinh90"] = d["High"].shift(1).rolling(N).max()
d["Day90"] = d["Low"].shift(1).rolling(N).min()
d = d[d["Date"] >= d["Date"].max() - pd.DateOffset(months=6)].copy()
d["x"] = mdates.date2num(d["Date"])
fig, (ax, av) = plt.subplots(2, 1, figsize=(12, 6.5), sharex=True, gridspec_kw={"height_ratios": [3.2, 1]})
candles(ax, d, 0.7)
ax.plot(d.x, d.MA7, color="#1f77b4", lw=1, label="MA7")
ax.plot(d.x, d.MA30, color="#ff7f0e", lw=1, label="MA30")
ax.plot(d.x, d.Dinh90, color="black", lw=.8, ls="--", label=f"Đỉnh {N} ngày trước đó")
ax.plot(d.x, d.Day90, color="gray", lw=.8, ls=":", label=f"Đáy {N} ngày trước đó")
up = d[d.Close > d.Dinh90]; dn = d[d.Close < d.Day90]
ax.scatter(up.x, up.High * 1.02, marker="^", s=45, color="black", zorder=4, label="Vượt đỉnh")
ax.scatter(dn.x, dn.Low * 0.98, marker="v", s=45, color="#d62728", zorder=4, label="Rớt đáy")
ax.set_ylabel("USD"); ax.grid(alpha=.3); ax.legend(fontsize=8, ncol=3, loc="upper left")
ax.set_title(f"Biểu đồ nến ngày 6 tháng gần nhất (đến {d.Date.max():%d/%m/%Y})")
av.bar(d.x, d.Volume, width=0.7, color=np.where(d.Close >= d.Open, UP, DOWN))
av.set_ylabel("Khối lượng (BTC)"); av.grid(alpha=.3)
av.xaxis.set_major_locator(mdates.MonthLocator())
av.xaxis.set_major_formatter(mdates.DateFormatter("%m/%Y"))
fig.tight_layout(); fig.savefig("outputs/candle_daily_6m.png", dpi=200); plt.close(fig)
print("Da luu outputs/candle_monthly.png va outputs/candle_daily_6m.png")
