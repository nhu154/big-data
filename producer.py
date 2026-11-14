import json, time, requests
from confluent_kafka import Producer

API = "https://api.binance.com/api/v3/klines"
TOPIC = "btc_klines_1m"
producer = Producer({"bootstrap.servers": "localhost:9092"})
last_sent = None

def on_delivery(err, msg):
    if err:
        print("LỖI gửi:", err)
    else:
        print(f"OK partition={msg.partition()} offset={msg.offset()} key={msg.key().decode()}")

while True:
    try:
        r = requests.get(API, params={"symbol": "BTCUSDT", "interval": "1m", "limit": 2}, timeout=10)
        r.raise_for_status()
        k = r.json()[0]
        if k[0] != last_sent:
            msg = {
                "source": "binance_rest", "symbol": "BTCUSDT",
                "open_time_ms": k[0], "open": float(k[1]), "high": float(k[2]),
                "low": float(k[3]), "close": float(k[4]), "volume": float(k[5]),
                "ingested_at": int(time.time() * 1000),
            }
            producer.produce(TOPIC, key=str(k[0]), value=json.dumps(msg), callback=on_delivery)
            producer.poll(0)
            last_sent = k[0]
    except Exception as e:
        print("Lỗi gọi API:", e)
    producer.poll(0)
    time.sleep(20)
