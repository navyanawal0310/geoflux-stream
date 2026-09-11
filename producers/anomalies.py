import copy
import random

from datetime import datetime, timezone, timedelta


def should_generate(probability):
    return random.random() < probability


def make_late_event(event, max_delay_seconds):
    late_event = copy.deepcopy(event)

    delay = random.randint(5, max_delay_seconds)

    event_time = datetime.now(timezone.utc) - timedelta(
        seconds=delay
    )

    late_event["event_time"] = event_time.isoformat()

    late_event["simulation_metadata"] = {
        "condition": "late",
        "delay_seconds": delay,
    }

    return late_event


def make_duplicate(event):
    duplicate = copy.deepcopy(event)

    duplicate["simulation_metadata"] = {
        "condition": "duplicate"
    }

    # IMPORTANT:
    # event_id intentionally remains unchanged.
    return duplicate


def make_malformed_event(event):
    malformed = copy.deepcopy(event)

    failure_type = random.choice([
        "missing_value",
        "missing_sensor_id",
        "invalid_value",
    ])

    if failure_type == "missing_value":
        malformed.pop("value", None)

    elif failure_type == "missing_sensor_id":
        malformed.pop("sensor_id", None)

    elif failure_type == "invalid_value":
        malformed["value"] = "INVALID"

    malformed["simulation_metadata"] = {
        "condition": "malformed",
        "failure_type": failure_type,
    }

    return malformed