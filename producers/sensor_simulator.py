import json
import random
import time
import uuid

from datetime import datetime, timezone
from collections import defaultdict

from confluent_kafka import Producer

from config import (
    KAFKA_BROKER,
    RAW_TOPIC,
    SENSOR_COUNT,
    EVENTS_PER_SECOND,
    LATE_EVENT_RATE,
    DUPLICATE_EVENT_RATE,
    MALFORMED_EVENT_RATE,
    MAX_EVENT_DELAY_SECONDS,
)

from sensors import (
    generate_sensor_network,
    generate_measurement,
)

from anomalies import (
    should_generate,
    make_late_event,
    make_duplicate,
    make_malformed_event,
)

producer = Producer(
    {
        "bootstrap.servers": KAFKA_BROKER,
        "client.id": "geoflux-sensor-network",
    }
)


sensors = generate_sensor_network(SENSOR_COUNT)

sequence_numbers = defaultdict(int)


def create_event(sensor):

    sensor_id = sensor["sensor_id"]

    sequence_numbers[sensor_id] += 1

    measurement = generate_measurement(
        sensor["sensor_type"]
    )

    return {
        "event_id": str(uuid.uuid4()),

        "sensor_id": sensor_id,
        "sequence_number": sequence_numbers[sensor_id],

        "sensor_type": sensor["sensor_type"],

        "city": sensor["city"],
        "country": sensor["country"],
        "region": sensor["region"],

        "latitude": sensor["latitude"],
        "longitude": sensor["longitude"],

        "event_time": datetime.now(
            timezone.utc
        ).isoformat(),

        "value": measurement["value"],
        "unit": measurement["unit"],

        "battery": random.randint(50, 100),

        "signal_strength": random.randint(
            -95,
            -40,
        ),
    }


def delivery_report(err, msg):

    if err is not None:

        print(
            f"DELIVERY FAILED: {err}"
        )


def print_network_summary():

    print("=" * 70)

    print("GEOFlux Distributed Sensor Network")

    print("=" * 70)

    print(
        f"Sensors:           {len(sensors)}"
    )

    print(
        f"Target throughput: {EVENTS_PER_SECOND} events/sec"
    )

    print(
        f"Kafka topic:       {RAW_TOPIC}"
    )

    print("=" * 70)

    print()

def publish_event(event):

    key = event.get("sensor_id", "UNKNOWN")

    producer.produce(
        RAW_TOPIC,
        key=key,
        value=json.dumps(event),
        callback=delivery_report,
    )

    producer.poll(0)

print_network_summary()


interval = 1 / EVENTS_PER_SECOND

event_count = 0


try:

    while True:

        sensor = random.choice(sensors)

        event = create_event(sensor)

        condition = "normal"

        # Malformed event
        if should_generate(MALFORMED_EVENT_RATE):

            event = make_malformed_event(event)
            condition = "MALFORMED"


        # Late event
        elif should_generate(LATE_EVENT_RATE):

            event = make_late_event(
                event,
                MAX_EVENT_DELAY_SECONDS,
            )

            condition = "LATE"


        publish_event(event)


        # Duplicate is published twice
        if should_generate(DUPLICATE_EVENT_RATE):

            duplicate = make_duplicate(event)

            publish_event(duplicate)

            condition = "DUPLICATE"

        producer.poll(0)

        event_count += 1

        print(
            f'{event_count:<6} | '
            f'{event.get("sensor_id", "MISSING"):<15} | '
            f'{event.get("region", "UNKNOWN"):<7} | '
            f'{event.get("city", "UNKNOWN"):<15} | '
            f'{event.get("sensor_type", "UNKNOWN"):<12} | '
            f'{str(event.get("value", "MISSING")):<8} | '
            f'{condition}'
    )

        time.sleep(interval)


except KeyboardInterrupt:

    print("\nStopping GEOFlux simulator...")


finally:

    producer.flush()