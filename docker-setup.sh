#!/bin/bash
set -e

# Build Docker image
docker build -t example-fastapi .

# Run container
docker run -d --name example-fastapi -p 8000:8000 example-fastapi

echo "App running in Docker container at http://localhost:8000"
