from confluent_kafka import Consumer

c = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "logger",
    "auto.offset.reset": "earliest"
})
c.subscribe(["prices"])

try:
    while True:
        msg = c.poll(1.0)
        if msg is None or msg.error():
            continue
        print("LOG:", msg.value().decode())
finally:
    c.close()
