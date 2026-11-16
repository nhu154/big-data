"""Quy trinh 7 (Decision/Action): dich vu FastAPI doc ket qua that trong outputs/."""
from pathlib import Path
import numpy as np
import pandas as pd
from fastapi import FastAPI, HTTPException, Query

BASE = Path("outputs")  # chay tu thu muc goc cua repo
VOL_WINDOW, VOL_Z = 30, 3.0
DD_WINDOW, DD_PCT = 90, -30.0
BREAK_N = 90                      # vuot dinh / rot day so voi 90 ngay truoc do
DISCLAIMER = "Thong tin tu du lieu lich su, khong phai khuyen nghi dau tu."

app = FastAPI(title="Bitcoin Decision API - Nhom 9",
              description="Canh bao rui ro va du bao 36 thang tu pipeline Kafka-Parquet-Spark.")


def clean(v):
    if isinstance(v, (np.floating, float)):
        return None if not np.isfinite(v) else round(float(v), 4)
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.bool_,)):
        return bool(v)
    if isinstance(v, pd.Timestamp):
        return v.strftime("%Y-%m-%d")
    return v


def records(df):
    return [{k: clean(v) for k, v in row.items()} for row in df.to_dict("records")]


def load_daily():
    f = BASE / "btc_daily.csv"
    if not f.exists():
        raise HTTPException(500, f"Khong thay {f}. Hay chay aggregate.py truoc.")
    d = pd.read_csv(f, parse_dates=["Date"])
    d = d[d["Record_Count"] >= 1440].sort_values("Date").reset_index(drop=True)
    r = d["Daily_Return_Pct"]
    d["Vol30"] = r.shift(1).rolling(VOL_WINDOW).std()
    d["Z"] = r / d["Vol30"]
    d["Dinh90"] = d["Close"].rolling(DD_WINDOW, min_periods=30).max()
    d["Giam_so_voi_dinh90_pct"] = (d["Close"] / d["Dinh90"] - 1) * 100
    d["Canh_bao_bien_dong"] = d["Z"].abs() > VOL_Z
    d["Canh_bao_giam_sau"] = d["Giam_so_voi_dinh90_pct"] <= DD_PCT
    d["Dinh_truoc"] = d["High"].shift(1).rolling(BREAK_N).max()
    d["Day_truoc"] = d["Low"].shift(1).rolling(BREAK_N).min()
    d["Vuot_dinh"] = d["Close"] > d["Dinh_truoc"]
    d["Rot_day"] = d["Close"] < d["Day_truoc"]
    return d


@app.get("/")
def root():
    return {"ten": app.title, "endpoints": ["/health", "/latest", "/alerts", "/breakouts", "/forecast", "/model-evaluation", "/docs"],
            "luat": {"bien_dong": f"|loi suat ngay| > {VOL_Z} x do lech chuan {VOL_WINDOW} ngay truoc",
                     "giam_sau": f"gia giam >= {abs(DD_PCT):.0f}% so voi dinh {DD_WINDOW} ngay",
                     "vuot_dinh": f"gia dong cua > High cao nhat {BREAK_N} ngay truoc",
                     "rot_day": f"gia dong cua < Low thap nhat {BREAK_N} ngay truoc"},
            "luu_y": DISCLAIMER}


@app.get("/health")
def health():
    d = load_daily()
    return {"status": "ok", "so_ngay": len(d), "tu": d["Date"].min().strftime("%Y-%m-%d"),
            "den": d["Date"].max().strftime("%Y-%m-%d")}


@app.get("/latest")
def latest():
    d = load_daily()
    x = d.iloc[-1]
    canh_bao = []
    if x["Canh_bao_bien_dong"]:
        canh_bao.append(f"BIEN DONG BAT THUONG: loi suat {x['Daily_Return_Pct']:.2f}% (z={x['Z']:.1f})")
    if x["Canh_bao_giam_sau"]:
        canh_bao.append(f"GIAM SAU: {x['Giam_so_voi_dinh90_pct']:.1f}% so voi dinh {DD_WINDOW} ngay")
    if x["Vuot_dinh"]:
        canh_bao.append(f"VUOT DINH {BREAK_N} NGAY: {x['Close']:,.0f} > {x['Dinh_truoc']:,.0f} USD")
    if x["Rot_day"]:
        canh_bao.append(f"ROT DAY {BREAK_N} NGAY: {x['Close']:,.0f} < {x['Day_truoc']:,.0f} USD")
    xu_huong = "MA7 > MA30 (ngan han cao hon dai han)" if x["MA7"] > x["MA30"] else "MA7 <= MA30"
    out = {"ngay": x["Date"], "gia_dong_cua": x["Close"], "loi_suat_ngay_pct": x["Daily_Return_Pct"],
           "MA7": x["MA7"], "MA30": x["MA30"], "xu_huong": xu_huong, "do_lech_chuan_30_ngay": x["Vol30"],
           "giam_so_voi_dinh90_pct": x["Giam_so_voi_dinh90_pct"],
           "trang_thai": "CANH BAO" if canh_bao else "BINH THUONG", "luu_y": DISCLAIMER}
    return {k: clean(v) for k, v in out.items()} | {"canh_bao": canh_bao}


@app.get("/alerts")
def alerts(days: int = Query(365, ge=1, le=6000, description="Xem lai bao nhieu ngay gan nhat")):
    d = load_daily().tail(days)
    a = d[d["Canh_bao_bien_dong"] | d["Canh_bao_giam_sau"]]
    cols = ["Date", "Close", "Daily_Return_Pct", "Z", "Giam_so_voi_dinh90_pct", "Canh_bao_bien_dong", "Canh_bao_giam_sau"]
    return {"so_ngay_xet": len(d), "so_ngay_canh_bao": len(a), "danh_sach": records(a[cols])}


@app.get("/breakouts")
def breakouts(days: int = Query(365, ge=1, le=6000, description="Xem lai bao nhieu ngay gan nhat")):
    d = load_daily().tail(days)
    up, dn = d[d["Vuot_dinh"]], d[d["Rot_day"]]
    cols = ["Date", "Close", "Dinh_truoc", "Day_truoc", "Daily_Return_Pct"]
    return {"so_ngay_xet": len(d), "cua_so_ngay": BREAK_N,
            "so_lan_vuot_dinh": len(up), "so_lan_rot_day": len(dn),
            "vuot_dinh": records(up[cols]), "rot_day": records(dn[cols]),
            "luu_y": DISCLAIMER}


@app.get("/forecast")
def forecast(months: int = Query(12, ge=1, le=36, description="So thang sau ngay cuoi chuoi (1-36)")):
    f = BASE / "forecast_36m.csv"
    if not f.exists():
        raise HTTPException(500, f"Khong thay {f}. Hay chay forecast.py truoc.")
    fc = pd.read_csv(f)
    r = fc.iloc[months - 1]
    return {"thang_du_bao": r["Date"][:7], "so_thang_sau": months,
            "trung_vi": clean(r["Median"]), "dai_80pct": [clean(r["P10"]), clean(r["P90"])],
            "dai_50pct": [clean(r["P25"]), clean(r["P75"])],
            "phuong_phap": "Holt damped + bootstrap tren log gia dong cua thang",
            "luu_y": "Dai bat dinh rat rong; day la pham vi kha di, khong phai gia chac chan."}


@app.get("/model-evaluation")
def model_evaluation():
    f = BASE / "ml_metrics.csv"
    if not f.exists():
        raise HTTPException(500, f"Khong thay {f}. Hay chay ml_spark.py truoc.")
    m = pd.read_csv(f)
    return {"bang": records(m),
            "ket_luan": "Khong mo hinh nao vuot muc co so 'luon doan tang' tren tap test; "
                        "vi vay dich vu nay chi canh bao rui ro, khong du doan huong gia ngay mai."}
