import os, time
import numpy as np, pandas as pd
from statsmodels.tsa.holtwinters import ExponentialSmoothing

t0 = time.time()
os.makedirs("outputs", exist_ok=True)
H = 36  # so thang du bao = 3 nam

# ---------- chuoi gia dong cua THANG (log) ----------
daily = pd.read_csv("outputs/btc_daily.csv", parse_dates=["Date"])
full = daily[daily["Record_Count"] >= 1440]                      # bo ngay chua du du lieu
m_close = full.set_index("Date")["Close"].resample("MS").last()
if (full["Date"].max() + pd.Timedelta(days=1)).day != 1:         # bo thang cuoi neu chua ket thuc
    m_close = m_close.iloc[:-1]
y_log = np.log(m_close)
print(f"Chuoi thang: {len(m_close)} diem, {m_close.index[0]:%m/%Y} -> {m_close.index[-1]:%m/%Y}")
print(f"Gia dong cua thang cuoi: {m_close.iloc[-1]:,.0f} USD")

# ---------- bon phuong phap ----------
def f_naive(tr, h): return np.repeat(tr.iloc[-1], h)

def f_drift48(tr, h):
    t = tr.iloc[-48:]
    return tr.iloc[-1] + (t.iloc[-1] - t.iloc[0]) / (len(t) - 1) * np.arange(1, h + 1)

def f_trend_all(tr, h):
    b, a = np.polyfit(np.arange(len(tr)), tr.values, 1)
    return a + b * (len(tr) - 1 + np.arange(1, h + 1))

def f_holt(tr, h):
    fit = ExponentialSmoothing(tr.values, trend="add", damped_trend=True,
                               initialization_method="estimated").fit()
    return np.asarray(fit.forecast(h))

MODELS = {"Naive (giu gia cuoi)": f_naive, "Drift 48 thang": f_drift48,
          "Xu huong tuyen tinh (toan bo)": f_trend_all, "Holt damped": f_holt}

# ---------- backtest cuon chieu ----------
rows = []
for h in (12, 36):
    origins = list(range(len(y_log) - h, 60, -6))
    for name, f in MODELS.items():
        mae = [np.mean(np.abs(f(y_log.iloc[:o], h) - y_log.iloc[o:o + h].values)) for o in origins]
        rows.append({"Horizon_thang": h, "Mo_hinh": name, "MAE_log": round(np.mean(mae), 4),
                     "Sai_so_pct_dien_hinh": round((np.exp(np.mean(mae)) - 1) * 100, 1), "So_moc": len(origins)})
bt = pd.DataFrame(rows)
bt.to_csv("outputs/forecast_backtest.csv", index=False)
print("\nBACKTEST:\n" + bt.to_string(index=False))

# ---------- dai bat dinh bang bootstrap ----------
rng = np.random.default_rng(42)
def fan(tr, center, win=60, n=5000):
    r = tr.diff().dropna().iloc[-win:]; r = r - r.mean()
    return center[None, :] + rng.choice(r.values, size=(n, len(center))).cumsum(axis=1)

cov = []
for h in (12, 36):
    hit = []
    for o in range(len(y_log) - h, 72, -3):
        tr, te = y_log.iloc[:o], y_log.iloc[o:o + h].values
        q10, q90 = np.percentile(fan(tr, f_holt(tr, h), n=1000), [10, 90], axis=0)
        hit.append(np.mean((te >= q10) & (te <= q90)))
    cov.append({"Horizon_thang": h, "Dai_80pct_chua_gia_that": round(np.mean(hit), 3), "So_moc": len(hit)})
cov = pd.DataFrame(cov)
cov.to_csv("outputs/forecast_coverage.csv", index=False)
print("\nDO TIN CAY CUA DAI 80%:\n" + cov.to_string(index=False))

# ---------- du bao 36 thang tu toan bo du lieu ----------
sims = np.exp(fan(y_log, f_holt(y_log, H), n=5000))
q = {p: np.percentile(sims, p, axis=0) for p in (10, 25, 50, 75, 90)}
dates = pd.date_range(m_close.index[-1] + pd.offsets.MonthBegin(1), periods=H, freq="MS")
out = pd.DataFrame({"Date": dates.strftime("%Y-%m-%d"), "P10": q[10].round(0), "P25": q[25].round(0),
                    "Median": q[50].round(0), "P75": q[75].round(0), "P90": q[90].round(0)})
out.to_csv("outputs/forecast_36m.csv", index=False)
print("\nDU BAO (trung vi) va dai 80%:")
for k in (12, 24, 36):
    r = out.iloc[k - 1]
    print(f"  Sau {k:>2} thang ({r['Date'][:7]}): {r['Median']:>9,.0f} USD   dai 80%: {r['P10']:,.0f} - {r['P90']:,.0f}")

# ---------- bieu do ----------
try:
    import matplotlib; matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    plt.rcParams["font.family"] = "DejaVu Sans"   # co san trong matplotlib, ho tro tieng Viet co dau
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(m_close.index, m_close.values, color="#1f4e79", label="Giá đóng cửa tháng (thực tế)")
    ax.fill_between(dates, q[10], q[90], color="#9ecae1", alpha=0.5, label="Dải 80%")
    ax.fill_between(dates, q[25], q[75], color="#6baed6", alpha=0.6, label="Dải 50%")
    ax.plot(dates, q[50], color="#e6550d", label="Dự báo (trung vị)")
    ax.set_yscale("log"); ax.set_ylabel("USD (thang log)"); ax.grid(alpha=0.3); ax.legend()
    ax.set_title("Dự báo giá Bitcoin 36 tháng (Holt damped + bootstrap)")
    fig.tight_layout(); fig.savefig("outputs/forecast_chart.png", dpi=150)
    print("\nDa luu bieu do: outputs/forecast_chart.png")
except ImportError:
    print("\n(Chua cai matplotlib nen bo qua bieu do: pip install matplotlib)")
print(f"XONG trong {time.time()-t0:.1f}s")
