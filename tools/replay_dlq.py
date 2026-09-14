import json
from confluent_kafka import Consumer, Producer


BROKERS = "localhost:9092"
DLQ_TOPIC = "sensor.dlq"
REPLAY_TOPIC = "sensor.raw"
GROUP_ID = "geoflux-dlq-replay-tool"


consumer = Consumer({
    "bootstrap.servers": BROKERS,
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
})

producer = Producer({
    "bootstrap.servers": BROKERS,
})


def repair_event(dlq_record):
    event = dlq_record.get("parsed_event")

    if not isinstance(event, dict):
        return None
        current_replay_count = dlq_record.get("replay_count", 0)

        # Poison-event protection:
        # never automatically replay the same event more than once.
        if current_replay_count >= 1:
            print(
                f"REPLAY_LIMIT: event already replayed "
                f"{current_replay_count} time(s)"
            )
            return None

    failure_reason = dlq_record.get("failure_reason")
    failed_field = dlq_record.get("failed_field")

    # ----------------------------------------
    # Demonstration repair rules
    # ----------------------------------------

    if (
        failure_reason == "MISSING_REQUIRED_FIELD"
        and failed_field == "sensor_id"
    ):
        event["sensor_id"] = "RECOVERED-UNKNOWN"

    elif (
        failure_reason == "MISSING_REQUIRED_FIELD"
        and failed_field == "value"
    ):
        return None

    elif (
        failure_reason == "INVALID_VALUE"
        and failed_field == "value"
    ):
        return None

    else:
        return None

    # ----------------------------------------
    # Replay lineage metadata
    # ----------------------------------------

    metadata = event.get("replay_metadata", {})

    metadata["replayed"] = True
    metadata["original_failure_reason"] = failure_reason
    metadata["replay_count"] = current_replay_count + 1

    event["replay_metadata"] = metadata

    return event


def delivery_report(err, msg):

    if err:
        print(f"Replay failed: {err}")

    else:
        print(
            f"Replayed → "
            f"{msg.topic()} "
            f"partition={msg.partition()} "
            f"offset={msg.offset()}"
        )


def main():

    consumer.subscribe([DLQ_TOPIC])

    print("Reading quarantined events...")

    try:

        while True:

            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                print(msg.error())
                continue

            try:
                dlq_record = json.loads(
                    msg.value().decode("utf-8")
                )

            except Exception as exc:
                print(
                    f"Could not parse DLQ record: {exc}"
                )
                continue

            repaired = repair_event(dlq_record)

            if repaired is None:
                print(
                    "SKIP:",
                    dlq_record.get("failure_reason"),
                    dlq_record.get("failed_field"),
                )

                consumer.commit(
                    message=msg,
                    asynchronous=False,
                )

                continue

            producer.produce(
                REPLAY_TOPIC,
                key=repaired.get(
                    "event_id",
                    "",
                ).encode("utf-8"),
                value=json.dumps(
                    repaired
                ).encode("utf-8"),
                callback=delivery_report,
            )

            producer.flush()

            consumer.commit(
                message=msg,
                asynchronous=False,
            )

    except KeyboardInterrupt:
        print("\nReplay stopped.")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()