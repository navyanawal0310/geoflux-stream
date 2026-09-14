from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from database import query


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