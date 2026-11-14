import glob, os, sqlite3
import pandas as pd

os.makedirs("dbdata", exist_ok=True)
db_path = "dbdata/dashboard.db"
if os.path.exists(db_path):
    os.remove(db_path)                       # tao lai tu dau moi lan chay
con = sqlite3.connect(db_path)

files = sorted(glob.glob("outputs/*.csv"))
if not files:
    raise SystemExit("Khong thay file nao trong outputs/. Hay chay lai aggregate.py truoc.")
for path in files:
    name = os.path.splitext(os.path.basename(path))[0]
    df = pd.read_csv(path)
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"])          # de Superset nhan ra la cot thoi gian
    df.to_sql(name, con, index=False)
    print(f"{name:32s} {len(df):>9,} dong, {len(df.columns)} cot")
con.close()
print(f"\nXONG -> {db_path} ({os.path.getsize(db_path)/1e6:.1f} MB)")
