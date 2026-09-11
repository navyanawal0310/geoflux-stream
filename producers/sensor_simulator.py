import json
import random
import time
from datetime import datetime, timezone

from confluent_kafka import Producer


KAFKA_BROKER = "localhost:9092"
TOPIC = "sensor.raw"


producer = Producer(
    {
        "bootstrap.servers": KAFKA_BROKER,
        "client.id": "geoflux-sensor-simulator",
    }
)


SENSORS = [
    {
        "sensor_id": "BLR-WEATHER-001",
        "sensor_type": "weather",
        "city": "Bengaluru",
        "country": "India",
        "region": "APAC",
        "latitude": 12.9716,
        "longitude": 77.5946,
    },
    {
        "sensor_id": "TOK-SEISMIC-001",
        "sensor_type": "seismic",
        "city": "Tokyo",
        "country": "Japan",
        "region": "APAC",
        "latitude": 35.6762,
        "longitude": 139.6503,
    },
    {
        "sensor_id": "LON-AIR-001",
        "sensor_type": "air_quality",
        "city": "London",
        "country": "United Kingdom",
        "region": "EU",
        "latitude": 51.5072,
        "longitude": -0.1276,
    },
]


def generate_value(sensor_type):
    if sensor_type == "weather":
        return round(random.uniform(20, 35), 2)

    if sensor_type == "seismic":
        return round(random.uniform(0.0, 2.0), 3)

    if sensor_type == "air_quality":
        return round(random.uniform(10, 150), 2)

    return round(random.uniform(0, 100), 2)


def create_event(sensor):
    return {
        "sensor_id": sensor["sensor_id"],
        "sensor_type": sensor["sensor_type"],
        "city": sensor["city"],
        "country": sensor["country"],
        "region": sensor["region"],
        "latitude": sensor["latitude"],
        "longitude": sensor["longitude"],
        "event_time": datetime.now(timezone.utc).isoformat(),
        "value": generate_value(sensor["sensor_type"]),
        "battery": random.randint(60, 100),
        "signal_strength": random.randint(-90, -40),
    }


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(
            f"Delivered → "
            f"topic={msg.topic()} "
            f"partition={msg.partition()} "
            f"offset={msg.offset()}"
        )


print("GEOFlux sensor simulator started.")
print("Press CTRL+C to stop.\n")


try:
    while True:
        sensor = random.choice(SENSORS)

        event = create_event(sensor)

        producer.produce(
            TOPIC,
            key=event["sensor_id"],
            value=json.dumps(event),
            callback=delivery_report,
        )

        producer.poll(0)

        print(
            f'{event["sensor_id"]:<20} '
            f'{event["sensor_type"]:<12} '
            f'value={event["value"]}'
        )

        time.sleep(1)

except KeyboardInterrupt:
    print("\nStopping sensor simulator...")

finally:
    producer.flush()