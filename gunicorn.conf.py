import os

bind = os.getenv("BIND", "0.0.0.0:3000")
# Default 1 worker avoids SQLite write contention; set WEB_CONCURRENCY higher when using PostgreSQL.
workers = int(os.getenv("WEB_CONCURRENCY", "1"))
threads = int(os.getenv("GUNICORN_THREADS", "1"))
timeout = int(os.getenv("GUNICORN_TIMEOUT", "120"))
accesslog = "-"
errorlog = "-"
capture_output = True
enable_stdio_inheritance = True
