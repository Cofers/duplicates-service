# Gunicorn configuration file
import multiprocessing

# Server socket
bind = "0.0.0.0:8080"

# Worker processes
workers = 2
worker_class = "uvicorn.workers.UvicornWorker"

# Timeout
timeout = 120

# Logging
accesslog = "-"
errorlog = "-"
loglevel = "info"

# Process naming
proc_name = "duplicates-service" 