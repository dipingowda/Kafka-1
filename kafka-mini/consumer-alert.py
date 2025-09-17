from confluent_kafka import Consumer
import json, collections

c = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "analytics",
    "auto.offset.reset": "earliest"
})
c.subscribe(["prices"])

window = collections.deque(maxlen=5)

try:
    while True:
        msg = c.poll(1.0)
        if msg is None or msg.error():
            continue
        data = json.loads(msg.value().decode())
        price = data["price"]
        window.append(price)
        if len(window) == window.maxlen:
            avg = sum(window)/len(window)
            if avg and abs(price-avg)/avg > 0.01:
                print(f"ALERT: {data['ticker']} moved >1% vs MA5 | last={price:.2f}, MA5={avg:.2f}")
finally:
    c.close()
