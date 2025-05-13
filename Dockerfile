# Use an official Python runtime as a parent image
FROM python:3.11-slim-bookworm as requirements-stage

# Install poetry
RUN pip install poetry==1.8.5

# Copy only the files needed for poetry to install dependencies
COPY ./pyproject.toml ./poetry.lock ./

# Generate requirements.txt
RUN poetry export -f requirements.txt --output requirements.txt --without-hashes --without=dev

# Start a new stage from the slim version of the parent image
FROM python:3.11-slim-bookworm

# Set the working directory in the container to /app
WORKDIR /app

# Copy the requirements.txt from the previous stage
COPY --from=requirements-stage /requirements.txt /requirements.txt

# Install any needed packages specified in requirements.txt
RUN python3 -m pip install --no-cache-dir --upgrade -r /requirements.txt

# Copy the current directory contents into the container at /app
COPY . .

# Make port 8080 available to the world outside this container
ENV PORT=8080
EXPOSE 8080

# Define environment variable to store the directory for shared memory
ENV SHM_DIR=/tmp/shm
RUN mkdir -p ${SHM_DIR} && mkdir -p /.local

# Set PYTHONPATH to include the current directory
ENV PYTHONPATH=/app

# Run the application
CMD ["gunicorn", "-k", "uvicorn.workers.UvicornWorker", "-c", "gunicorn.conf.py", "main:app"] 