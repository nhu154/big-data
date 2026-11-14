import pandas as pd

daily = pd.read_csv("outputs/btc_daily.csv", parse_dates=["Date"])
full = daily[daily["Record_Count"] >= 1440]
m = full.set_index("Date")["Close"].resample("MS").last()
if (full["Date"].max() + pd.Timedelta(days=1)).day != 1:
    m = m.iloc[:-1]                                   # bo thang chua ket thuc (giong forecast.py)

actual = pd.DataFrame({"Date": m.index.strftime("%Y-%m-%d"), "Loai": "Thuc te", "Close": m.values.round(0)})
fc = pd.read_csv("outputs/forecast_36m.csv")
fc.insert(1, "Loai", "Du bao")
out = pd.concat([actual, fc], ignore_index=True)
# noi duong thuc te voi duong du bao: thang cuoi thuc te cung la diem xuat phat cua dai
last = out["Loai"].eq("Thuc te").values.nonzero()[0][-1]
for c in ("P10", "P25", "Median", "P75", "P90"):
    out.loc[last, c] = out.loc[last, "Close"]
out.to_csv("outputs/dash_forecast.csv", index=False)
print(f"Da luu outputs/dash_forecast.csv: {len(out)} dong ({len(actual)} thuc te + {len(fc)} du bao)")
print(out.iloc[[0, last, last + 1, -1]].to_string(index=False))
