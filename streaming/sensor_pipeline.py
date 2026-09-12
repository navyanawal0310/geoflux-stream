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


def evaluate_event(raw_event: str):

    try:
        event = json.loads(raw_event)

    except json.JSONDecodeError:
        return None

    required_fields = [
        "event_id",
        "sensor_id",
        "sensor_type",
        "event_time",
        "value",
    ]

    for field in required_fields:
        if field not in event:
            return None

    try:
        value = float(event["value"])
    except (ValueError, TypeError):
        return None

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

    if severity is None:
        return None

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
        "detected_at": datetime.now(
            timezone.utc
        ).isoformat(),
    }

    return json.dumps(alert)


def main():

    env = StreamExecutionEnvironment.get_execution_environment()

    env.set_parallelism(2)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(BROKERS)
        .set_topics(SOURCE_TOPIC)
        .set_group_id("geoflux-flink-alert-engine")
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

    alert_stream = (
        raw_stream
        .map(
            evaluate_event,
            output_type=Types.STRING(),
        )
        .filter(lambda event: event is not None)
    )

    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(BROKERS)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(ALERT_TOPIC)
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

    alert_stream.sink_to(sink)

    alert_stream.print()

    env.execute(
        "GEOFlux Real-Time Alert Engine"
    )


if __name__ == "__main__":
    main()