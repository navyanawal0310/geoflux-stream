import random


LOCATIONS = [
    # APAC
    {
        "city": "Bengaluru",
        "country": "India",
        "region": "APAC",
        "latitude": 12.9716,
        "longitude": 77.5946,
    },
    {
        "city": "Mumbai",
        "country": "India",
        "region": "APAC",
        "latitude": 19.0760,
        "longitude": 72.8777,
    },
    {
        "city": "Tokyo",
        "country": "Japan",
        "region": "APAC",
        "latitude": 35.6762,
        "longitude": 139.6503,
    },
    {
        "city": "Singapore",
        "country": "Singapore",
        "region": "APAC",
        "latitude": 1.3521,
        "longitude": 103.8198,
    },

    # Europe
    {
        "city": "London",
        "country": "United Kingdom",
        "region": "EU",
        "latitude": 51.5072,
        "longitude": -0.1276,
    },
    {
        "city": "Berlin",
        "country": "Germany",
        "region": "EU",
        "latitude": 52.5200,
        "longitude": 13.4050,
    },
    {
        "city": "Paris",
        "country": "France",
        "region": "EU",
        "latitude": 48.8566,
        "longitude": 2.3522,
    },

    # North America
    {
        "city": "New York",
        "country": "USA",
        "region": "NA",
        "latitude": 40.7128,
        "longitude": -74.0060,
    },
    {
        "city": "San Francisco",
        "country": "USA",
        "region": "NA",
        "latitude": 37.7749,
        "longitude": -122.4194,
    },
    {
        "city": "Toronto",
        "country": "Canada",
        "region": "NA",
        "latitude": 43.6532,
        "longitude": -79.3832,
    },

    # South America
    {
        "city": "Sao Paulo",
        "country": "Brazil",
        "region": "SA",
        "latitude": -23.5505,
        "longitude": -46.6333,
    },
    {
        "city": "Buenos Aires",
        "country": "Argentina",
        "region": "SA",
        "latitude": -34.6037,
        "longitude": -58.3816,
    },

    # Africa
    {
        "city": "Cape Town",
        "country": "South Africa",
        "region": "AFRICA",
        "latitude": -33.9249,
        "longitude": 18.4241,
    },
    {
        "city": "Nairobi",
        "country": "Kenya",
        "region": "AFRICA",
        "latitude": -1.2921,
        "longitude": 36.8219,
    },
    {
        "city": "Cairo",
        "country": "Egypt",
        "region": "AFRICA",
        "latitude": 30.0444,
        "longitude": 31.2357,
    },
]


SENSOR_TYPES = {
    "weather": {
        "unit": "celsius",
        "min": -5,
        "max": 42,
    },
    "air_quality": {
        "unit": "pm2.5",
        "min": 5,
        "max": 180,
    },
    "seismic": {
        "unit": "m/s2",
        "min": 0,
        "max": 2,
    },
    "power": {
        "unit": "hz",
        "min": 49.5,
        "max": 50.5,
    },
}


def generate_sensor_network(sensor_count):
    sensors = []

    sensor_types = list(SENSOR_TYPES.keys())

    for index in range(1, sensor_count + 1):

        location = random.choice(LOCATIONS)

        sensor_type = random.choice(sensor_types)

        city_code = (
            location["city"]
            .upper()
            .replace(" ", "")[:3]
        )

        sensor_code = sensor_type.upper()[:3]

        sensor_id = (
            f"{city_code}-{sensor_code}-{index:04d}"
        )

        sensor = {
            "sensor_id": sensor_id,
            "sensor_type": sensor_type,
            "city": location["city"],
            "country": location["country"],
            "region": location["region"],
            "latitude": location["latitude"],
            "longitude": location["longitude"],
        }

        sensors.append(sensor)

    return sensors


def generate_measurement(sensor_type):

    config = SENSOR_TYPES[sensor_type]

    value = random.uniform(
        config["min"],
        config["max"],
    )

    return {
        "value": round(value, 3),
        "unit": config["unit"],
    }