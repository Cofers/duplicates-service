# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set the working directory in the container to /app
WORKDIR /app

# Install poetry
RUN pip install poetry==1.8.5

# Copy project files
COPY pyproject.toml poetry.lock* ./
COPY src/ src/
COPY main.py .
COPY gunicorn.conf.py .

# Install dependencies
RUN poetry install --no-dev --no-interaction --no-ansi

# Make port 8080 available to the world outside this container
ENV PORT 8080
EXPOSE 8080

# Run the application with uvicorn worker
ENTRYPOINT ["gunicorn", "-k", "uvicorn.workers.UvicornWorker", "-c", "gunicorn.conf.py", "main:app"] 