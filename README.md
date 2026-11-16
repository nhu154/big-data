# Phân tích và dự báo giá Bitcoin với pipeline dữ liệu lớn 7 quy trình

Đồ án môn **Nhập môn Dữ liệu lớn** - Trường Đại học Sài Gòn - Nhóm 9.
Giáo viên hướng dẫn: Lê Nhị Lãm Thúy.

| MSSV | Họ tên |
|---|---|
| 3123411014 | Nguyễn Hoàng Vân Anh |
| 3123411196 | Hoàng Kim Ngân |
| 3123411336 | Lê Đỗ Tâm Uyên |
| 3123411215 | Trịnh Tâm Như |
| 3123411316 | Nguyễn Lương Tấn Trung |

## Giới thiệu

Dự án xây dựng một pipeline dữ liệu lớn hoàn chỉnh trên dữ liệu giá Bitcoin theo từng phút
(bộ [Bitcoin Historical Data](https://www.kaggle.com/datasets/mczielinski/bitcoin-historical-data) trên Kaggle,
7.805.067 dòng, 378 MB, từ 01/2012 đến 11/2026), đi qua đủ 7 quy trình:

```
Sources → Ingestion → Storage → Processing → Analytics → Presentation → Decision/Action
Kaggle     Kafka       Parquet     Spark        ML, dự báo   Superset       FastAPI, Gmail
```

| Quy trình | Chương trình | Đầu vào | Đầu ra |
|---|---|---|---|
| 1. Sources | tải từ Kaggle hoặc Release của repo | Kaggle | `data/btcusd_1-min_data.csv` |
| 2. Ingestion | `src/pipeline/producer.py`, `backfill.py` | tệp CSV | topic Kafka `btc_klines_1m` |
| 3. Storage | `src/pipeline/to_parquet.py` | topic Kafka | `datalake/btc_raw` (Parquet, phân vùng theo năm) |
| 4. Processing | `src/pipeline/clean.py`, `aggregate.py` (PySpark) | `datalake/btc_raw` | `datalake/btc_clean`, `reports/quality_report.json`, `outputs/btc_*.csv` |
| 5. Analytics | `src/analytics/analytics_sql.py`, `src/ml/ml_spark.py`, `src/ml/forecast.py` | `btc_clean`, `btc_daily.csv` | `outputs/analytics_*`, `ml_*`, `forecast_*` |
| 6. Presentation | `src/analytics/make_dashboard_data.py`, `src/pipeline/to_sqlite.py`, Apache Superset | `outputs/*.csv` | dashboard 10 biểu đồ |
| 7. Decision/Action | `src/serving/api.py` (FastAPI), `src/monitoring/notify.py` | `outputs/*.csv` | API cảnh báo, bản tin Gmail hằng ngày |

## Kết quả chính

- **Chất lượng dữ liệu:** không thiếu giá trị, không trùng thời điểm, không có khoảng trống thời gian; 16,81% số phút có khối lượng bằng 0 (nến phẳng, được giữ lại).
- **Spark so với pandas:** trên cùng phép gom nến phút thành nến ngày, Spark nhanh hơn khoảng 2 lần ở mọi quy mô (1x đến 8x dữ liệu), trong khi bộ nhớ của pandas tăng theo dữ liệu.
- **Học máy:** Logistic Regression, Decision Tree, Random Forest dự đoán hướng giá ngày kế tiếp đạt Accuracy 0,48 - 0,50, **không vượt mức cơ sở 0,5083** (luôn đoán tăng). Kết luận: không dùng mô hình làm tín hiệu giao dịch.
- **Dự báo 36 tháng (Holt damped):** trung vị khoảng 88.036 / 96.972 / 103.031 USD sau 12 / 24 / 36 tháng, dải 80% rất rộng (32.963 - 319.706 USD ở năm thứ ba).
- **Cảnh báo:** vượt đỉnh 90 ngày, rớt đáy 90 ngày, biến động bất thường (|Z| > 3), giảm sâu (≤ -30% so với đỉnh 90 ngày); gửi bản tin qua Gmail lúc 8:05 mỗi sáng.

## Cấu trúc thư mục

```
big-data/
├── src/
│   ├── pipeline/     producer, backfill, to_parquet, clean, aggregate, to_sqlite, rebuild_csv
│   ├── analytics/    analytics_sql, make_dashboard_data, outlier_price, candle_chart
│   ├── ml/           ml_spark, ml_spark_exp, forecast
│   ├── serving/      api (FastAPI)
│   ├── monitoring/   notify (cảnh báo, bản tin Gmail), alert_eval
│   ├── dashboard/    superset_config
│   └── utils/        explain_catalyst, benchmark, spark_test
├── outputs/          17 tệp kết quả (CSV, biểu đồ dự báo, env_info.txt)
├── reports/          quality_report.json, catalyst_plan.txt
├── archive/          bản nén mã nguồn và kết quả cũ
├── requirements.txt
└── .env.example
```

`datalake/` và tệp CSV gốc không đưa lên repo vì dung lượng lớn; tệp CSV có trong mục **Releases**.

## Cách chạy

Yêu cầu: Python 3.11 trở lên, Java 17 trở lên (cho Spark), Docker Desktop (cho Kafka và Superset).
Mọi lệnh đều chạy **từ thư mục gốc của repo**.

```bash
python3 -m venv venv
source venv/bin/activate          # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

1. **Sources:** tải `btcusd_1-min_data.csv` từ Releases (hoặc Kaggle) vào thư mục `data/`.
2. **Ingestion:**
   ```bash
   docker run -d --name kafka -p 9092:9092 apache/kafka:latest
   python src/pipeline/producer.py
   ```
3. **Storage:** `python src/pipeline/to_parquet.py`
4. **Processing:**
   ```bash
   python src/pipeline/clean.py
   python src/pipeline/aggregate.py
   python src/utils/benchmark.py            # so sánh pandas và Spark
   python src/utils/explain_catalyst.py     # xem kế hoạch tối ưu của Catalyst
   ```
5. **Analytics:**
   ```bash
   python src/analytics/analytics_sql.py
   python src/ml/ml_spark.py
   python src/ml/forecast.py
   ```
6. **Presentation:**
   ```bash
   python src/analytics/make_dashboard_data.py
   python src/pipeline/to_sqlite.py
   docker start superset                    # mở http://localhost:8088
   ```
7. **Decision/Action:**
   ```bash
   uvicorn src.serving.api:app --port 8000  # tài liệu API: http://127.0.0.1:8000/docs
   cp .env.example .env                     # điền thông tin Gmail vào .env
   set -a; source .env; set +a              # nạp biến môi trường từ .env
   python src/monitoring/notify.py --kaggle --daily
   ```

## Bảo mật

Mật khẩu ứng dụng Gmail và mã bot Telegram chỉ đặt trong biến môi trường hoặc tệp `.env` trên máy,
**không đưa lên repo**. `.gitignore` đã loại `.env`, `*.plist`, `kaggle.json`, `reports/notify_state.json`, `notify.log`.

## Lưu ý

Bộ dữ liệu trên Kaggle được cập nhật hằng ngày. Số liệu trong báo cáo lấy từ bản tải ngày 03/11/2026
(bản ghi cuối lúc 04:27 UTC); chạy lại với bản mới hơn thì số liệu sẽ thay đổi nhẹ nhưng các kết luận chính không đổi.
