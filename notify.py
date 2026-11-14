"""Quy trinh 7 (Decision/Action): kiem tra canh bao va gui thong bao qua Telegram va/hoac email.
Cach chay:
  python notify.py --date 2026-02-05  # kiem tra mot ngay trong qua khu (demo)
  python notify.py --kaggle --daily   # lay ban moi nhat cua bo du lieu Kaggle, gui ban tin moi ngay
  python notify.py --live --daily     # lay nen ngay moi nhat tu API Bitstamp, gui ban tin moi ngay
  python notify.py --dry-run          # chi in noi dung, khong gui
Bien moi truong: TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, SMTP_USER, SMTP_PASS (App Password), EMAIL_TO
"""
import argparse, json, os, smtplib, ssl, urllib.parse, urllib.request
from email.mime.text import MIMEText
from pathlib import Path
import pandas as pd
try:
    import truststore
    truststore.inject_into_ssl()
except ImportError:
    pass

BASE = Path(__file__).resolve().parent
BREAK_N, VOL_WINDOW, VOL_Z, DD_WINDOW, DD_PCT = 90, 30, 3.0, 90, -30.0
STATE = BASE / "reports" / "notify_state.json"
BITSTAMP = "https://www.bitstamp.net/api/v2/ohlc/btcusd/?step=86400&limit=1000&exclude_current_candle=true"
KAGGLE_DATASET = "mczielinski/bitcoin-historical-data"


def fetch_bitstamp():
    req = urllib.request.Request(BITSTAMP, headers={"User-Agent": "btc-nhom9-notify"})
    with urllib.request.urlopen(req, timeout=20) as r:
        b = pd.DataFrame(json.load(r)["data"]["ohlc"])
    for c in ("timestamp", "open", "high", "low", "close", "volume"):
        b[c] = pd.to_numeric(b[c])
    b = b[b["timestamp"] + 86400 <= pd.Timestamp.now(tz="UTC").timestamp()]
    return pd.DataFrame({"Date": pd.to_datetime(b["timestamp"], unit="s").dt.normalize(),
                         "Open": b["open"], "High": b["high"], "Low": b["low"],
                         "Close": b["close"], "Volume": b["volume"], "Record_Count": 1440})


def fetch_kaggle(after):
    import kagglehub
    csv = next(Path(kagglehub.dataset_download(KAGGLE_DATASET)).rglob("*.csv"))
    cut = (after + pd.Timedelta(days=1)).timestamp()
    m = pd.concat([c[c["Timestamp"] >= cut] for c in pd.read_csv(csv, chunksize=1_000_000)])
    m["Date"] = pd.to_datetime(m["Timestamp"], unit="s").dt.normalize()
    g = m.sort_values("Timestamp").groupby("Date").agg(
        Open=("Open", "first"), High=("High", "max"), Low=("Low", "min"),
        Close=("Close", "last"), Volume=("Volume", "sum"), Record_Count=("Timestamp", "count")).reset_index()
    print(f"Kaggle: phut cuoi {pd.to_datetime(m['Timestamp'].max(), unit='s'):%d/%m/%Y %H:%M} UTC")
    return g[g["Record_Count"] >= 1440]


def load(source=None):
    d = pd.read_csv(BASE / "outputs" / "btc_daily.csv", parse_dates=["Date"])
    d = d[d["Record_Count"] >= 1440].sort_values("Date").reset_index(drop=True)
    if source:
        last = d["Date"].max()
        b = fetch_kaggle(last) if source == "kaggle" else fetch_bitstamp()
        new = b[b["Date"] > last]
        print(f"{source}: {len(new)} ngay moi sau {last:%d/%m/%Y}")
        d = pd.concat([d[["Date", "Open", "High", "Low", "Close", "Volume", "Record_Count"]], new], ignore_index=True)
        d["Daily_Return_Pct"] = d["Close"].pct_change() * 100
        (BASE / "reports").mkdir(exist_ok=True)
        d.to_csv(BASE / "reports" / "btc_daily_live.csv", index=False)
    r = d["Daily_Return_Pct"]
    d["Dinh_N"] = d["High"].shift(1).rolling(BREAK_N).max()
    d["Day_N"] = d["Low"].shift(1).rolling(BREAK_N).min()
    d["Z"] = r / r.shift(1).rolling(VOL_WINDOW).std()
    d["Dinh_DD"] = d["Close"].rolling(DD_WINDOW, min_periods=30).max()
    d["Giam_pct"] = (d["Close"] / d["Dinh_DD"] - 1) * 100
    d["Vuot_dinh"] = d["Close"] > d["Dinh_N"]
    d["Rot_day"] = d["Close"] < d["Day_N"]
    d["Bien_dong"] = d["Z"].abs() > VOL_Z
    d["Giam_sau"] = d["Giam_pct"] <= DD_PCT
    return d


def build_message(x):
    lines = []
    if x["Vuot_dinh"]:
        lines.append(f"🚀 VƯỢT ĐỈNH {BREAK_N} NGÀY: đóng cửa {x.Close:,.0f} USD > đỉnh cũ {x.Dinh_N:,.0f} USD")
    if x["Rot_day"]:
        lines.append(f"🔻 RỚT ĐÁY {BREAK_N} NGÀY: đóng cửa {x.Close:,.0f} USD < đáy cũ {x.Day_N:,.0f} USD")
    if x["Bien_dong"]:
        lines.append(f"⚠️ BIẾN ĐỘNG BẤT THƯỜNG: lợi suất {x.Daily_Return_Pct:+.2f}% (z = {x.Z:.1f})")
    if x["Giam_sau"]:
        lines.append(f"📉 GIẢM SÂU: thấp hơn đỉnh {DD_WINDOW} ngày {abs(x.Giam_pct):.1f}%")
    head = f"[BTC Nhóm 9] Ngày {x.Date:%d/%m/%Y} – giá đóng cửa {x.Close:,.0f} USD ({x.Daily_Return_Pct:+.2f}%)"
    tail = "Thông tin từ dữ liệu lịch sử, không phải khuyến nghị đầu tư."
    info = f"Đỉnh {BREAK_N} ngày: {x.Dinh_N:,.0f} USD · đáy {BREAK_N} ngày: {x.Day_N:,.0f} USD · cách đỉnh {x.Giam_pct:+.1f}%"
    return lines, "\n".join([head, *lines, tail]) if lines else "\n".join([head, "Không có cảnh báo.", info, tail])


def send_telegram(text):
    tok, chat = os.getenv("TELEGRAM_BOT_TOKEN"), os.getenv("TELEGRAM_CHAT_ID")
    if not (tok and chat):
        print("Bỏ qua Telegram: chưa đặt TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID"); return False
    data = urllib.parse.urlencode({"chat_id": chat, "text": text}).encode()
    with urllib.request.urlopen(f"https://api.telegram.org/bot{tok}/sendMessage", data=data, timeout=15) as r:
        ok = json.load(r).get("ok", False)
    print("Telegram:", "đã gửi" if ok else "lỗi"); return ok


def send_email(subject, text):
    user, pw, to = os.getenv("SMTP_USER"), os.getenv("SMTP_PASS"), os.getenv("EMAIL_TO")
    if not (user and pw and to):
        print("Bỏ qua email: chưa đặt SMTP_USER / SMTP_PASS / EMAIL_TO"); return False
    msg = MIMEText(text, "plain", "utf-8"); msg["Subject"], msg["From"], msg["To"] = subject, user, to
    host, port = os.getenv("SMTP_HOST", "smtp.gmail.com"), int(os.getenv("SMTP_PORT", "465"))
    with smtplib.SMTP_SSL(host, port, context=ssl.create_default_context(), timeout=20) as s:
        s.login(user, pw); s.sendmail(user, [a.strip() for a in to.split(",")], msg.as_string())
    print("Email: đã gửi tới", to); return True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date"); ap.add_argument("--test", action="store_true"); ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--live", action="store_true"); ap.add_argument("--kaggle", action="store_true")
    ap.add_argument("--daily", action="store_true")
    a = ap.parse_args()
    d = load("kaggle" if a.kaggle else "bitstamp" if a.live else None)
    x = d[d["Date"] == pd.Timestamp(a.date)].iloc[0] if a.date else d.iloc[-1]
    alerts, text = build_message(x)
    print(text)
    if a.dry_run or not (alerts or a.test or a.daily):
        return
    state = json.loads(STATE.read_text()) if STATE.exists() else {}
    key = f"{x.Date:%Y-%m-%d}"
    if state.get("last_sent") == key and not a.test:
        print("Đã gửi cho ngày này, bỏ qua."); return
    subject = (f"[BTC Nhóm 9] {len(alerts)} cảnh báo ngày {x.Date:%d/%m/%Y}" if alerts
               else f"[BTC Nhóm 9] Bản tin ngày {x.Date:%d/%m/%Y}" if a.daily else "[BTC Nhóm 9] Tin nhắn thử")
    sent = False
    for fn, args in ((send_telegram, (text,)), (send_email, (subject, text))):
        try:
            sent = fn(*args) or sent
        except Exception as e:
            print(f"{fn.__name__} lỗi: {e}")
    if sent and not a.test:
        STATE.parent.mkdir(exist_ok=True); STATE.write_text(json.dumps({"last_sent": key}))


if __name__ == "__main__":
    main()
