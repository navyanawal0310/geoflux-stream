from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy


def main():

    env = StreamExecutionEnvironment.get_execution_environment()

    env.set_parallelism(2)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers("kafka:29092")
        .set_topics("sensor.raw")
        .set_group_id("geoflux-flink-consumer")
        .set_value_only_deserializer(
            SimpleStringSchema()
        )
        .build()
    )

    stream = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "Kafka Sensor Source",
    )

    stream.print()

    env.execute("GEOFlux Kafka Ingestion")


if __name__ == "__main__":
    main()