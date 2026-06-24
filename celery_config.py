import os

broker_url = os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379/0")
result_backend = os.environ.get("CELERY_RESULT_BACKEND", "redis://localhost:6379/1")

# Serialisation
task_serializer = "json"
result_serializer = "json"
accept_content = ["json"]

# Reliability
task_acks_late = True
worker_prefetch_multiplier = 1

# Timeouts - encoding can take a long time
task_time_limit = 86400       # 24 hours hard limit
task_soft_time_limit = 82800  # 23 hours soft limit

# Worker lifecyle
worker_max_tasks_per_child = 50   # restart after 50 tasks to prevent memory leaks
worker_disable_rate_limits = True

# Result expiry — discard results after 1 hour
result_expires = 3600
task_ignore_result = True

# Concurrency per worker is set at CLI start time,
# but you can set defaults here per queue if needed
