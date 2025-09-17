from confluent_kafka import Producer
import yfinance as yf, time, json, os

BOOTSTRAP = os.environ.get("BOOTSTRAP", "localhost:9092")
TOPIC = os.environ.get("TOPIC", "prices")
TICKER = os.environ.get("TICKER", "AAPL")   # you can set e.g. RELIANCE.NS

p = Producer({"bootstrap.servers": BOOTSTRAP})

def delivery_report(err, msg):
    if err:
        print("Delivery failed:", err)
    else:
        pass  # uncomment for verbose: print(f"Delivered to {msg.topic()} [{msg.partition()}] @ {msg.offset()}")

while True:
    price = yf.Ticker(TICKER).fast_info.last_price
    payload = {"ticker": TICKER, "price": float(price)}
    p.produce(TOPIC, json.dumps(payload).encode(), callback=delivery_report)
    p.poll(0)
    print("sent:", payload)
    time.sleep(2)
