import json
from datetime import datetime, timezone

from pyflink.common import Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.base import DeliveryGuarantee

from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaSink,
    KafkaRecordSerializationSchema,
    KafkaOffsetsInitializer,
)


BROKERS = "kafka:29092"

SOURCE_TOPIC = "sensor.raw"
ALERT_TOPIC = "sensor.alerts"
DLQ_TOPIC = "sensor.dlq"

PIPELINE_VERSION = "alert-engine-v2"


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def make_dlq_record(
    raw_event,
    failure_reason,
    parsed_event=None,
    failed_field=None,
):
    """
    Create a quarantine record for an event that cannot safely
    continue through the processing pipeline.
    """

    dlq_record = {
        "failure_reason": failure_reason,
        "failed_field": failed_field,
        "failed_stage": "validation",
        "failed_at": utc_now(),
        "source_topic": SOURCE_TOPIC,
        "pipeline_version": PIPELINE_VERSION,
        "replay_count": 0,
        "raw_event": raw_event,
        "parsed_event": parsed_event,
    }

    return dlq_record


def classify_event(raw_event: str):
    """
    Every incoming Kafka record is classified into exactly one route:

    ALERT
        Valid sensor event that crosses an alert threshold.

    IGNORE
        Valid sensor event that does not cross an alert threshold.

    DLQ
        Invalid event that violates the expected event contract.

    Nothing silently disappears anymore.
    """

    # ---------------------------------------------------------
    # 1. JSON validation
    # ---------------------------------------------------------

    try:
        event = json.loads(raw_event)

    except json.JSONDecodeError as exc:
        dlq = make_dlq_record(
            raw_event=raw_event,
            failure_reason="INVALID_JSON",
        )

        dlq["error_detail"] = str(exc)

        return json.dumps({
            "route": "DLQ",
            "payload": dlq,
        })

    # ---------------------------------------------------------
    # 2. Structural validation
    # ---------------------------------------------------------

    if not isinstance(event, dict):
        dlq = make_dlq_record(
            raw_event=raw_event,
            parsed_event=event,
            failure_reason="INVALID_EVENT_STRUCTURE",
        )

        return json.dumps({
            "route": "DLQ",
            "payload": dlq,
        })

    required_fields = [
        "event_id",
        "sensor_id",
        "sensor_type",
        "event_time",
        "value",
    ]

    for field in required_fields:
        if field not in event:
            dlq = make_dlq_record(
                raw_event=raw_event,
                parsed_event=event,
                failure_reason="MISSING_REQUIRED_FIELD",
                failed_field=field,
            )

            return json.dumps({
                "route": "DLQ",
                "payload": dlq,
            })

    # ---------------------------------------------------------
    # 3. Type validation
    # ---------------------------------------------------------

    try:
        value = float(event["value"])

    except (ValueError, TypeError):
        dlq = make_dlq_record(
            raw_event=raw_event,
            parsed_event=event,
            failure_reason="INVALID_VALUE",
            failed_field="value",
        )

        return json.dumps({
            "route": "DLQ",
            "payload": dlq,
        })

    # ---------------------------------------------------------
    # 4. Business-rule evaluation
    # ---------------------------------------------------------

    sensor_type = event["sensor_type"]

    severity = None
    reason = None

    if sensor_type == "seismic" and value > 1.7:
        severity = "HIGH"
        reason = "Elevated seismic activity"

    elif sensor_type == "air_quality" and value > 150:
        severity = "HIGH"
        reason = "Poor air quality"

    elif sensor_type == "weather" and value > 38:
        severity = "MEDIUM"
        reason = "Extreme temperature"

    elif sensor_type == "power" and (
        value < 49.7 or value > 50.3
    ):
        severity = "HIGH"
        reason = "Power frequency deviation"

    # ---------------------------------------------------------
    # Valid event, but nothing interesting happened.
    # Do NOT send this to the DLQ.
    # ---------------------------------------------------------

    if severity is None:
        return json.dumps({
            "route": "IGNORE",
            "payload": {
                "event_id": event["event_id"],
            },
        })

    # ---------------------------------------------------------
    # 5. Alert creation
    # ---------------------------------------------------------

    alert = {
        "alert_id": f'alert-{event["event_id"]}',
        "event_id": event["event_id"],
        "sensor_id": event["sensor_id"],
        "sensor_type": sensor_type,
        "city": event.get("city"),
        "country": event.get("country"),
        "region": event.get("region"),
        "value": value,
        "unit": event.get("unit"),
        "severity": severity,
        "reason": reason,
        "event_time": event["event_time"],
        "detected_at": utc_now(),
        "pipeline_version": PIPELINE_VERSION,
    }

    return json.dumps({
        "route": "ALERT",
        "payload": alert,
    })


def get_route(record: str):
    return json.loads(record)["route"]


def get_payload(record: str):
    return json.dumps(
        json.loads(record)["payload"]
    )


def build_kafka_sink(topic: str):

    return (
        KafkaSink.builder()
        .set_bootstrap_servers(BROKERS)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(topic)
            .set_value_serialization_schema(
                SimpleStringSchema()
            )
            .build()
        )
        .set_delivery_guarantee(
            DeliveryGuarantee.AT_LEAST_ONCE
        )
        .build()
    )


def main():

    env = StreamExecutionEnvironment.get_execution_environment()

    env.set_parallelism(2)

    # ---------------------------------------------------------
    # Kafka source
    # ---------------------------------------------------------

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(BROKERS)
        .set_topics(SOURCE_TOPIC)
        .set_group_id(
            "geoflux-flink-alert-engine"
        )
        .set_starting_offsets(
            KafkaOffsetsInitializer.latest()
        )
        .set_value_only_deserializer(
            SimpleStringSchema()
        )
        .build()
    )

    raw_stream = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "Kafka Sensor Source",
    )

    # ---------------------------------------------------------
    # Validation / routing stage
    #
    # One record enters.
    # One classification leaves.
    # ---------------------------------------------------------

    classified_stream = raw_stream.map(
        classify_event,
        output_type=Types.STRING(),
    )

    # ---------------------------------------------------------
    # ALERT branch
    # ---------------------------------------------------------

    alert_stream = (
        classified_stream
        .filter(
            lambda record:
            get_route(record) == "ALERT"
        )
        .map(
            get_payload,
            output_type=Types.STRING(),
        )
    )

    # ---------------------------------------------------------
    # DLQ / quarantine branch
    # ---------------------------------------------------------

    dlq_stream = (
        classified_stream
        .filter(
            lambda record:
            get_route(record) == "DLQ"
        )
        .map(
            get_payload,
            output_type=Types.STRING(),
        )
    )

    # ---------------------------------------------------------
    # Kafka sinks
    # ---------------------------------------------------------

    alert_sink = build_kafka_sink(
        ALERT_TOPIC
    )

    dlq_sink = build_kafka_sink(
        DLQ_TOPIC
    )

    alert_stream.sink_to(
        alert_sink
    ).name(
        "Kafka Alert Sink"
    )

    dlq_stream.sink_to(
        dlq_sink
    ).name(
        "Kafka DLQ Sink"
    )

    # Useful while developing.
    alert_stream.print(
        "ALERT"
    )

    dlq_stream.print(
        "DLQ"
    )

    env.execute(
        "GEOFlux Alert + Quarantine Engine"
    )


if __name__ == "__main__":
    main()