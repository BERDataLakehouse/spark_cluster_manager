# CDM Spark Cluster Manager API

A simple API service for managing Spark clusters in Kubernetes for JupyterHub users. This service acts as an intermediary between user pods and Kubernetes, providing a secure way to manage Spark clusters without requiring direct Kubernetes access.

> **⚠️ Important:** This API service is intended to be deployed only within a Kubernetes environment.


## Features

- Create and manage Spark clusters for individual users
- RESTful API endpoints for cluster management
- Automatic cleanup of resources
- KBase user authentication and authorization

## Setup Local Development Environment

### Prerequisites

- Python 3.11+
- pip

### Installation

1. Clone the repository:
   ```
   git clone git@github.com:kbase/cdm-kube-spark-manager.git
   cd cdm-kube-spark-manager
   ```

2. Install dependencies:
   ```
   uv sync --locked # only the first time or when uv.lock changes
   ```

## Local Unit Testing

The project uses pytest for unit testing. To run the tests:

```bash
# Run all tests
PYTHONPATH=src pytest -v --cov=src --cov-report=xml tests
```

## API Endpoints

### Health Check
- `GET /health` - Check the health status of the service

### Cluster Management
- `POST /clusters` - Creates a new Spark cluster for the authenticated user
- `GET /clusters` - Get the status of the Spark cluster for the authenticated user, if it exists
- `DELETE /clusters` - Deletes the Spark cluster belonging to the authenticated user.
## Kubernetes Deployment

For instructions on deploying to local Kubernetes, see the [Kubernetes README](kubernetes/README.md).

> **Note:** The Docker image installs dependencies using `pyproject.toml` for modern Python packaging best practices. For local development, you can use either `requirements.txt` or `pyproject.toml` as shown above.

