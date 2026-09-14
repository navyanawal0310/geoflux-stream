from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import requests
import socket
from database import query
from confluent_kafka import Consumer, TopicPartition
app = FastAPI(
    title="GEOFlux Intelligence API",
    version="0.1.0",
)


app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/health")
def health():
    result = query("SELECT 1 AS status")

    return {
        "service": "geoflux-api",
        "status": "healthy",
        "database": "clickhouse",
        "database_status": result[0]["status"],
    }


@app.get("/api/incidents/summary")
def incident_summary():

    rows = query(
        """
        SELECT
            count() AS total_incidents,
            uniqExact(city) AS affected_cities,
            countIf(severity = 'CRITICAL') AS critical_incidents,
            countIf(severity = 'HIGH') AS high_incidents
        FROM incidents
        """
    )

    return rows[0]


@app.get("/api/incidents/by-type")
def incidents_by_type():

    return query(
        """
        SELECT
            incident_type,
            count() AS incident_count
        FROM incidents
        GROUP BY incident_type
        ORDER BY incident_count DESC
        """
    )


@app.get("/api/incidents/by-city")
def incidents_by_city():

    return query(
        """
        SELECT
            city,
            count() AS incident_count,
            countIf(severity = 'CRITICAL') AS critical_count,
            countIf(severity = 'HIGH') AS high_count
        FROM incidents
        GROUP BY city
        ORDER BY incident_count DESC
        LIMIT 20
        """
    )


@app.get("/api/incidents/recent")
def recent_incidents():

    return query(
        """
        SELECT
            incident_type,
            city,
            sensor_type,
            severity,
            event_count,
            max_value,
            average_value,
            window_start,
            window_end,
            detected_at
        FROM incidents
        ORDER BY detected_at DESC
        LIMIT 50
        """
    )


@app.get("/api/incidents/timeline")
def incident_timeline():

    return query(
        """
        SELECT
            toStartOfMinute(window_start) AS timestamp,
            count() AS incident_count
        FROM incidents
        WHERE window_start >= now() - INTERVAL 60 MINUTE
        GROUP BY timestamp
        ORDER BY timestamp
        """
    )

@app.get("/api/incidents/map")
def incident_map():

    return query(
        """
        SELECT
            i.city,
            d.country,
            d.region,
            d.latitude,
            d.longitude,

            count() AS incident_count,

            countIf(
                i.severity = 'CRITICAL'
            ) AS critical_count,

            countIf(
                i.severity = 'HIGH'
            ) AS high_count,

            max(i.detected_at) AS latest_incident,

            argMax(
                i.incident_type,
                i.detected_at
            ) AS latest_incident_type,

            argMax(
                i.severity,
                i.detected_at
            ) AS latest_severity

        FROM incidents AS i

        INNER JOIN city_dimension AS d
            ON i.city = d.city

        WHERE
            i.detected_at >=
            now() - INTERVAL 60 MINUTE

        GROUP BY
            i.city,
            d.country,
            d.region,
            d.latitude,
            d.longitude

        ORDER BY incident_count DESC
        """
    )
@app.get("/api/incidents/live")
def live_incidents():

    return query(
        """
        SELECT
            i.incident_type,
            i.city,
            d.country,
            d.region,
            d.latitude,
            d.longitude,

            i.sensor_type,
            i.severity,

            i.event_count,
            i.max_value,
            i.min_value,
            i.average_value,

            i.window_start,
            i.window_end,
            i.detected_at

        FROM incidents AS i

        INNER JOIN city_dimension AS d
            ON i.city = d.city

        ORDER BY i.detected_at DESC

        LIMIT 100
        """
    )
@app.get("/api/system/health")
def system_health():

    services = {
        "api": {
            "status": "UP"
        }
    }

    # ClickHouse
    try:
        result = query(
            "SELECT 1 AS status"
        )

        services["clickhouse"] = {
            "status": "UP"
            if result[0]["status"] == 1
            else "DOWN"
        }

    except Exception as exc:
        services["clickhouse"] = {
            "status": "DOWN",
            "error": str(exc)
        }


    # Flink
    try:
        response = requests.get(
            "http://localhost:8081/overview",
            timeout=2
        )

        data = response.json()

        services["flink"] = {
            "status": "UP",
            "taskmanagers":
                data.get(
                    "taskmanagers",
                    0
                ),
            "slots_total":
                data.get(
                    "slots-total",
                    0
                ),
            "slots_available":
                data.get(
                    "slots-available",
                    0
                ),
            "jobs_running":
                data.get(
                    "jobs-running",
                    0
                )
        }

    except Exception as exc:
        services["flink"] = {
            "status": "DOWN",
            "error": str(exc)
        }


    # Kafka TCP connectivity
    try:
        connection = socket.create_connection(
            ("localhost", 9092),
            timeout=2
        )

        connection.close()

        services["kafka"] = {
            "status": "UP"
        }

    except Exception as exc:
        services["kafka"] = {
            "status": "DOWN",
            "error": str(exc)
        }


    return services
@app.get("/api/system/throughput")
def system_throughput():

    rows = query(
        """
        SELECT
            toStartOfSecond(detected_at) AS timestamp,
            count() AS events
        FROM incidents
        WHERE detected_at >= now() - INTERVAL 60 SECOND
        GROUP BY timestamp
        ORDER BY timestamp
        """
    )

    total = sum(
        row["events"]
        for row in rows
    )

    return {
        "events_last_60_seconds": total,
        "events_per_second": round(total / 60, 2),
        "timeline": rows
    }

@app.get("/api/system/kafka-lag")
def kafka_lag():

    topic = "sensor.raw"

    groups = [
        "geoflux-flink-alert-engine",
        "geoflux-window-engine",
    ]

    results = []

    for group_id in groups:

        consumer = Consumer(
            {
                "bootstrap.servers": "localhost:9092",
                "group.id": group_id,
                "enable.auto.commit": False,
                "session.timeout.ms": 6000,
            }
        )

        try:
            metadata = consumer.list_topics(
                topic=topic,
                timeout=5
            )

            topic_metadata = (
                metadata.topics.get(topic)
            )

            if topic_metadata is None:
                raise RuntimeError(
                    f"Topic {topic} not found"
                )

            partitions = [
                TopicPartition(
                    topic,
                    partition_id
                )
                for partition_id
                in topic_metadata.partitions.keys()
            ]


            committed = consumer.committed(
                    partitions,
                    timeout=5
                )


            total_lag = 0

            partition_metrics = []


            for tp in committed:

                low, high = (
                    consumer
                    .get_watermark_offsets(
                        TopicPartition(
                            topic,
                            tp.partition
                        ),
                        timeout=5
                    )
                )


                committed_offset = (
                    tp.offset
                    if tp.offset >= 0
                    else None
                )


                if committed_offset is None:
                    lag = None

                else:
                    lag = max(
                        high -
                        committed_offset,
                        0
                    )

                    total_lag += lag


                partition_metrics.append(
                    {
                        "partition":
                            tp.partition,

                        "committed_offset":
                            committed_offset,

                        "latest_offset":
                            high,

                        "lag":
                            lag,
                    }
                )


            results.append(
                {
                    "group_id":
                        group_id,

                    "topic":
                        topic,

                    "total_lag":
                        total_lag,

                    "partitions":
                        partition_metrics,
                }
            )


        except Exception as exc:

            results.append(
                {
                    "group_id":
                        group_id,

                    "topic":
                        topic,

                    "status":
                        "ERROR",

                    "error":
                        str(exc),
                }
            )


        finally:
            consumer.close()


    return {
        "topic": topic,
        "consumer_groups": results
    }
@app.get("/api/incidents/city/{city}")
def incidents_for_city(city: str):

    safe_city = city.replace("'", "''")

    incidents = query(
        f"""
        SELECT
            incident_type,
            city,
            sensor_type,
            severity,
            event_count,
            max_value,
            min_value,
            average_value,
            window_start,
            window_end,
            detected_at
        FROM incidents
        WHERE city = '{safe_city}'
        ORDER BY detected_at DESC
        LIMIT 50
        """
    )

    return {
        "city": city,
        "incidents": incidents
    }