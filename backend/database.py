import clickhouse_connect


CLICKHOUSE_CONFIG = {
    "host": "localhost",
    "port": 8123,
    "username": "geoflux",
    "password": "geoflux",
    "database": "geoflux",
}


def query(sql: str):
    client = clickhouse_connect.get_client(
        **CLICKHOUSE_CONFIG
    )

    try:
        result = client.query(sql)

        return [
            dict(
                zip(
                    result.column_names,
                    row
                )
            )
            for row in result.result_rows
        ]

    finally:
        client.close()