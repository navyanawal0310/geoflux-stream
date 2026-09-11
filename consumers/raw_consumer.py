import json
from confluent_kafka import Consumer, KafkaError


TOPIC = "sensor.raw"

consumer = Consumer(
    {
        "bootstrap.servers": "localhost:9092",
        "group.id": "geoflux-raw-consumers",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    }
)

consumer.subscribe([TOPIC])

print("GEOFlux raw consumer started.")
print(f"Listening to: {TOPIC}")
print("Press CTRL+C to stop.\n")


try:
    while True:

        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue

            print(f"Kafka error: {msg.error()}")
            continue

        event = json.loads(msg.value().decode("utf-8"))

        print(
            f'RECEIVED | '
            f'partition={msg.partition()} '
            f'offset={msg.offset():<5} | '
            f'{event["sensor_id"]:<20} '
            f'{event["city"]:<12} '
            f'{event["sensor_type"]:<12} '
            f'value={event["value"]}'
        )

except KeyboardInterrupt:
    print("\nStopping GEOFlux consumer...")

finally:
    consumer.close()