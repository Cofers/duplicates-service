# Use Python 3.11 slim image as base
FROM python:3.11-slim

# Set working directory
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

CMD ["gunicorn", "main:app", "-c", "gunicorn.conf.py"] 