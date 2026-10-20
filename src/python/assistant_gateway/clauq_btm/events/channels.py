EVENTS_CHANNEL_PREFIX = "clauq:events:"
ALL_EVENTS_CHANNEL = "clauq:events:*"


def get_events_channel(queue_id: str) -> str:
    return f"{EVENTS_CHANNEL_PREFIX}{queue_id}"
