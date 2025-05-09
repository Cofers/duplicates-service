# Duplicates Service

A service for detecting duplicate transactions using FastAPI, Redis, and Google Cloud services.

## Features

- REST API for transaction processing
- Real-time duplicate detection
- Integration with Google Cloud BigQuery for analytics
- Pub/Sub integration for event-driven architecture
- Redis for caching and temporary storage

## Prerequisites

- Python 3.9 or higher
- Poetry for dependency management
- Redis server
- Google Cloud credentials

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/duplicates-service.git
cd duplicates-service
```

2. Install dependencies:
```bash
poetry install
```

3. Set up environment variables:
```bash
cp .env.example .env
# Edit .env with your configuration
```

## Development

1. Start the development server:
```bash
poetry run uvicorn src.duplicates_service.api.main:app --reload
```

2. Run tests:
```bash
poetry run pytest
```

3. Format code:
```bash
poetry run black .
poetry run isort .
```

## Project Structure

```
duplicates-service/
├── src/
│   └── duplicates_service/
│       ├── api/            # FastAPI routes and endpoints
│       ├── core/           # Core business logic
│       ├── models/         # Pydantic models
│       ├── services/       # External service integrations
│       └── utils/          # Utility functions
├── tests/                  # Test files
├── pyproject.toml          # Poetry configuration
└── README.md              # Project documentation
```

## API Documentation

Once the server is running, you can access the API documentation at:
- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

## License

This project is licensed under the MIT License - see the LICENSE file for details.