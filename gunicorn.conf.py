# Gunicorn configuration file
# Server socket
bind = "0.0.0.0:8080"

# Worker processes
workers = 2
threads = 8
worker_class = "uvicorn.workers.UvicornWorker"

# Timeout
timeout = 300

# Logging
accesslog = "-"
errorlog = "-"
loglevel = "info"

# Process naming
proc_name = "duplicates-service" 