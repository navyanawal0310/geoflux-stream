import json
from datetime import datetime

from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.base import DeliveryGuarantee
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaSink,
    KafkaRecordSerializationSchema,
    KafkaOffsetsInitializer,
)
from pyflink.common.watermark_strategy import (
    WatermarkStrategy,
    TimestampAssigner,
)
from pyflink.datastream.functions import AggregateFunction
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.common import Types, Time, Duration

BROKERS = "kafka:29092"
SOURCE_TOPIC = "sensor.raw"
INCIDENT_TOPIC = "geo.incidents"


def parse_event(raw_event):

    try:
        event = json.loads(raw_event)

        if not all(
            field in event
            for field in [
                "event_id",
                "sensor_id",
                "sensor_type",
                "city",
                "event_time",
                "value",
            ]
        ):
            return None

        event["value"] = float(event["value"])

        return event

    except (json.JSONDecodeError, ValueError, TypeError):
        return None


class EventTimestampAssigner(TimestampAssigner):

    def extract_timestamp(self, event, record_timestamp):

        timestamp = datetime.fromisoformat(
            event["event_time"].replace("Z", "+00:00")
        )

        return int(timestamp.timestamp() * 1000)


watermark_strategy = (
    WatermarkStrategy
    .for_bounded_out_of_orderness(
        Duration.of_seconds(5)
    )
    .with_timestamp_assigner(
        EventTimestampAssigner()
    )
)

class IncidentAggregate(AggregateFunction):

    def create_accumulator(self):
        # count, max_value, sum_value
        return 0, float("-inf"), 0.0

    def add(self, event, accumulator):

        count, max_value, total = accumulator

        value = event["value"]

        return (
            count + 1,
            max(max_value, value),
            total + value,
        )

    def get_result(self, accumulator):

        count, max_value, total = accumulator

        average = total / count if count else 0

        return (
            count,
            max_value,
            average,
        )

    def merge(self, a, b):

        return (
            a[0] + b[0],
            max(a[1], b[1]),
            a[2] + b[2],
        )


def classify_incident(record):

    key, statistics = record

    city, sensor_type = key

    count, max_value, average = statistics

    severity = None
    incident_type = None

    if sensor_type == "seismic":
        if max_value > 1.7 and count >= 3:
            severity = "CRITICAL"
            incident_type = "SEISMIC_CLUSTER"

    elif sensor_type == "air_quality":
        if average > 140 and count >= 3:
            severity = "HIGH"
            incident_type = "AIR_QUALITY_CLUSTER"

    elif sensor_type == "weather":
        if average > 35 and count >= 3:
            severity = "MEDIUM"
            incident_type = "EXTREME_HEAT_CLUSTER"

    elif sensor_type == "power":
        if (
            max_value > 50.3
            or average < 49.7
        ) and count >= 3:
            severity = "HIGH"
            incident_type = "POWER_INSTABILITY"

    if severity is None:
        return None

    incident = {
        "incident_type": incident_type,
        "city": city,
        "sensor_type": sensor_type,
        "event_count": count,
        "max_value": round(max_value, 3),
        "average_value": round(average, 3),
        "severity": severity,
        "window_seconds": 10,
    }

    return json.dumps(incident)


def main():

    env = StreamExecutionEnvironment.get_execution_environment()

    env.set_parallelism(2)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(BROKERS)
        .set_topics(SOURCE_TOPIC)
        .set_group_id("geoflux-window-engine")
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

    parsed_stream = (
        raw_stream
        .map(
            parse_event,
            output_type=Types.PICKLED_BYTE_ARRAY(),
        )
        .filter(lambda event: event is not None)
    )


    timed_stream = (
        parsed_stream
        .assign_timestamps_and_watermarks(
            watermark_strategy
        )
    )

    aggregated = (
        timed_stream
        .key_by(
            lambda event: (
                event["city"],
                event["sensor_type"],
            ),
            key_type=Types.PICKLED_BYTE_ARRAY(),
        )
        .window(
            TumblingEventTimeWindows.of(
                Time.seconds(10)
            )
        )
        .aggregate(
            IncidentAggregate(),
            accumulator_type=Types.TUPLE([
                Types.INT(),
                Types.DOUBLE(),
                Types.DOUBLE(),
            ]),
            output_type=Types.TUPLE([
                Types.INT(),
                Types.DOUBLE(),
                Types.DOUBLE(),
            ]),
        )
    )

    # Reattach key for classification.
    #
    # If your PyFlink version does not retain the key in this shape,
    # we will switch to AggregateFunction + ProcessWindowFunction next.
    #
    # For this first pass, keep output visible:
    aggregated.print()

    env.execute(
        "GEOFlux Event-Time Window Engine"
    )


if __name__ == "__main__":
    main()