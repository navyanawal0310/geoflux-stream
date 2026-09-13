KAFKA_BROKER = "localhost:9092"
RAW_TOPIC = "sensor.raw"

SENSOR_COUNT = 100

# Increase load so keyed windows contain enough events
EVENTS_PER_SECOND = 30

# Failure simulation
LATE_EVENT_RATE = 0.10
DUPLICATE_EVENT_RATE = 0.03
MALFORMED_EVENT_RATE = 0.02

MAX_EVENT_DELAY_SECONDS = 20