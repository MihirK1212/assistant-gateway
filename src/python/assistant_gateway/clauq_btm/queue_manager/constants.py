"""
Redis key prefixes and configuration constants for the Celery queue manager.
"""

# Redis key prefixes
TASK_KEY_PREFIX = "clauq:task:"  # Hash storing task data
QUEUE_KEY_PREFIX = "clauq:queue:"  # Sorted set for FIFO ordering
QUEUE_META_PREFIX = "clauq:queue_meta:"  # Hash for queue metadata
CELERY_TASK_PREFIX = "clauq:celery_task:"  # Maps task_id -> celery_task_id
EVENTS_CHANNEL_PREFIX = "clauq:events:"  # Pub/sub channel for events
ALL_EVENTS_CHANNEL = "clauq:events:*"  # Pattern for all events

# Task state TTL (7 days) - completed tasks are kept for reference
COMPLETED_TASK_TTL = 7 * 24 * 60 * 60
