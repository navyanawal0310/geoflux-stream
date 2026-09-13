import json
from datetime import datetime, timezone

from pyflink.common import Types, Time, Duration
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import (
    WatermarkStrategy,
    TimestampAssigner,
)

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import (
    AggregateFunction,
    ProcessWindowFunction,
)

from pyflink.datastream.window import TumblingEventTimeWindows

from pyflink.datastream.connectors.base import DeliveryGuarantee
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaSink,
    KafkaRecordSerializationSchema,
    KafkaOffsetsInitializer,
)


BROKERS = "kafka:29092"

SOURCE_TOPIC = "sensor.raw"
INCIDENT_TOPIC = "geo.incidents"


# ---------------------------------------------------------
# Parsing
# ---------------------------------------------------------

def parse_event(raw_event):

    try:
        event = json.loads(raw_event)

        required_fields = [
            "event_id",
            "sensor_id",
            "sensor_type",
            "city",
            "event_time",
            "value",
        ]

        if not all(
            field in event
            for field in required_fields
        ):
            return None

        event["value"] = float(
            event["value"]
        )

        return event

    except (
        json.JSONDecodeError,
        ValueError,
        TypeError,
    ):
        return None


# ---------------------------------------------------------
# Event-time extraction
# ---------------------------------------------------------

class EventTimestampAssigner(
    TimestampAssigner
):

    def extract_timestamp(
        self,
        event,
        record_timestamp,
    ):

        timestamp = datetime.fromisoformat(
            event["event_time"].replace(
                "Z",
                "+00:00",
            )
        )

        return int(
            timestamp.timestamp() * 1000
        )


# ---------------------------------------------------------
# Incremental aggregation
# ---------------------------------------------------------

class IncidentAggregate(
    AggregateFunction
):

    def create_accumulator(self):

        # count
        # maximum
        # minimum
        # sum

        return (
            0,
            float("-inf"),
            float("inf"),
            0.0,
        )

    def add(
        self,
        event,
        accumulator,
    ):

        (
            count,
            max_value,
            min_value,
            total,
        ) = accumulator

        value = event["value"]

        return (
            count + 1,
            max(max_value, value),
            min(min_value, value),
            total + value,
        )

    def get_result(
        self,
        accumulator,
    ):

        (
            count,
            max_value,
            min_value,
            total,
        ) = accumulator

        average = (
            total / count
            if count
            else 0
        )

        return (
            count,
            max_value,
            min_value,
            average,
        )

    def merge(
        self,
        a,
        b,
    ):

        return (
            a[0] + b[0],
            max(a[1], b[1]),
            min(a[2], b[2]),
            a[3] + b[3],
        )


# ---------------------------------------------------------
# Window metadata + incident classification
# ---------------------------------------------------------

class IncidentWindowFunction(
    ProcessWindowFunction
):

    def process(
        self,
        key,
        context,
        aggregates,
    ):

        statistics = next(
            iter(aggregates)
        )

        (
            count,
            max_value,
            min_value,
            average,
        ) = statistics

        city, sensor_type = key

        incident_type = None
        severity = None


        # ---------------------------
        # SEISMIC
        # ---------------------------

        if sensor_type == "seismic":

            if (
                count >= 3
                and max_value > 1.7
            ):

                incident_type = (
                    "SEISMIC_CLUSTER"
                )

                severity = "CRITICAL"


        # ---------------------------
        # AIR QUALITY
        # ---------------------------

        elif sensor_type == "air_quality":

            if (
                count >= 3
                and average > 140
            ):

                incident_type = (
                    "AIR_QUALITY_CLUSTER"
                )

                severity = "HIGH"


        # ---------------------------
        # WEATHER
        # ---------------------------

        elif sensor_type == "weather":

            if (
                count >= 3
                and average > 35
            ):

                incident_type = (
                    "EXTREME_HEAT_CLUSTER"
                )

                severity = "MEDIUM"


        # ---------------------------
        # POWER GRID
        # ---------------------------

        elif sensor_type == "power":

            if (
                count >= 3
                and (
                    max_value > 50.3
                    or min_value < 49.7
                )
            ):

                incident_type = (
                    "POWER_INSTABILITY"
                )

                severity = "HIGH"


        if incident_type is None:
            return


        window_start = datetime.fromtimestamp(
            context.window().start / 1000,
            tz=timezone.utc,
        ).isoformat()


        window_end = datetime.fromtimestamp(
            context.window().end / 1000,
            tz=timezone.utc,
        ).isoformat()


        incident = {

            "incident_type":
                incident_type,

            "city":
                city,

            "sensor_type":
                sensor_type,

            "severity":
                severity,

            "event_count":
                count,

            "max_value":
                round(max_value, 3),

            "min_value":
                round(min_value, 3),

            "average_value":
                round(average, 3),

            "window_start":
                window_start,

            "window_end":
                window_end,

            "window_seconds":
                10,

            "detected_at":
                datetime.now(
                    timezone.utc
                ).isoformat(),
        }

        yield json.dumps(incident)


# ---------------------------------------------------------
# Pipeline
# ---------------------------------------------------------

def main():

    env = (
        StreamExecutionEnvironment
        .get_execution_environment()
    )

    env.set_parallelism(2)


    # -----------------------------------------------------
    # Kafka Source
    # -----------------------------------------------------

    source = (
        KafkaSource.builder()

        .set_bootstrap_servers(
            BROKERS
        )

        .set_topics(
            SOURCE_TOPIC
        )

        .set_group_id(
            "geoflux-window-engine-v2"
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


    # -----------------------------------------------------
    # Parse
    # -----------------------------------------------------

    parsed_stream = (
        raw_stream

        .map(
            parse_event,
            output_type=(
                Types.PICKLED_BYTE_ARRAY()
            ),
        )

        .filter(
            lambda event:
                event is not None
        )
    )


    # -----------------------------------------------------
    # Watermarks
    # -----------------------------------------------------

    watermark_strategy = (
        WatermarkStrategy

        .for_bounded_out_of_orderness(
            Duration.of_seconds(5)
        )

        .with_timestamp_assigner(
            EventTimestampAssigner()
        )
    )


    timed_stream = (
        parsed_stream
        .assign_timestamps_and_watermarks(
            watermark_strategy
        )
    )


    # -----------------------------------------------------
    # Event-time window
    # -----------------------------------------------------

    incident_stream = (

        timed_stream

        .key_by(
            lambda event: (
                event["city"],
                event["sensor_type"],
            ),

            key_type=(
                Types.PICKLED_BYTE_ARRAY()
            ),
        )

        .window(
            TumblingEventTimeWindows.of(
                Time.seconds(10)
            )
        )

        .aggregate(
            IncidentAggregate(),

            window_function=(
                IncidentWindowFunction()
            ),

            accumulator_type=(
                Types.TUPLE([
                    Types.INT(),
                    Types.DOUBLE(),
                    Types.DOUBLE(),
                    Types.DOUBLE(),
                ])
            ),

            output_type=(
                Types.STRING()
            ),
        )
    )


    # -----------------------------------------------------
    # Kafka Sink
    # -----------------------------------------------------

    sink = (
        KafkaSink.builder()

        .set_bootstrap_servers(
            BROKERS
        )

        .set_record_serializer(

            KafkaRecordSerializationSchema
            .builder()

            .set_topic(
                INCIDENT_TOPIC
            )

            .set_value_serialization_schema(
                SimpleStringSchema()
            )

            .build()
        )

        .set_delivery_guarantee(
            DeliveryGuarantee
            .AT_LEAST_ONCE
        )

        .build()
    )


    incident_stream.sink_to(
        sink
    )


    # Useful while developing
    incident_stream.print()


    env.execute(
        "GEOFlux Event Intelligence Engine"
    )


if __name__ == "__main__":
    main()