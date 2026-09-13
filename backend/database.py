import clickhouse_connect


client = clickhouse_connect.get_client(
    host="localhost",
    port=8123,
    username="geoflux",
    password="geoflux",
    database="geoflux",
)


def query(sql: str):
    result = client.query(sql)

    return [
        dict(zip(result.column_names, row))
        for row in result.result_rows
    ]