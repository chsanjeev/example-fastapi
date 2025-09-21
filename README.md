# Example FastAPI + DuckDB

Small FastAPI application using DuckDB as a local embedded backend. The DB manager uses a
ThreadPoolExecutor and thread-local duckdb connections so the app can safely handle many
concurrent requests (configurable worker pool).

Requirements:

- Python 3.9+
- Install from `requirements.txt`

Quick start:

1. python -m venv .venv
2. source .venv/bin/activate
3. pip install -r requirements.txt
4. uvicorn app.main:app --reload

Run tests:

    pytest -q

Environment variables
---------------------

- EXAMPLE_FASTAPI_DB: path to the duckdb file (default: data.db)
- EXAMPLE_FASTAPI_MAX_WORKERS: number of threads for DB executor (default: 100)
 - EXAMPLE_FASTAPI_READY_RETRIES: number of attempts for readiness check (default: 3)
 - EXAMPLE_FASTAPI_READY_DELAY: seconds to wait between readiness attempts (default: 0.1)
 - EXAMPLE_FASTAPI_READY_TIMEOUT: overall timeout in seconds for the readiness check (default: 1.0)

Production notes
----------------

The app is designed to run as an ASGI service (uvicorn/gunicorn + uvicorn workers). The DBManager uses a thread pool and thread-local DuckDB connections. If you run multiple worker processes (gunicorn with multiple workers), each process will have its own pool and DuckDB file connection.

If you need containerization later, you can add a Dockerfile; for local development use the native Python/uvicorn flow above.
