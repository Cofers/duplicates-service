# Gunicorn configuration file
# Server socket
bind = "0.0.0.0:8080"

# Worker processes
workers = 2
threads = 8
worker_class = "uvicorn.workers.UvicornWorker"

# Timeout settings
timeout = 120  # Reduced from 300 to 120 seconds
keepalive = 5
graceful_timeout = 30

# Worker settings
max_requests = 1000
max_requests_jitter = 50
worker_connections = 1000

# Logging
accesslog = "-"
errorlog = "-"
loglevel = "info"

# Process naming
proc_name = "duplicates-service"

# Preload app
preload_app = True

# Worker lifecycle
worker_tmp_dir = "/dev/shm" 