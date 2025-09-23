# Example FastAPI + DuckDB & Snowflake

---

## Overview

This FastAPI application provides a flexible API for CRUD operations on items, supporting both DuckDB and Snowflake as backends. The database schema and API are fully dynamic, allowing arbitrary fields in item records.

---

## Features

- **Configurable backend:** Select DuckDB (default) or Snowflake via `DB_BACKEND` env var.
- **Dynamic schema:** Item fields are determined by client input; insert/update SQL is generated accordingly.
- **High concurrency:** Uses thread pool and thread-local connections for safe concurrent access.
- **Health/readiness endpoints:** `/health` and `/ready` for service monitoring.
- **Comprehensive error handling:** All database errors are returned as JSON and logged to `app_error.log`.
- **Flexible authentication for Snowflake:** Supports password, keypair, and OAuth2.0 login.

---

## Environment Setup

### Requirements

- Python 3.12+
- Install dependencies: `pip install -r requirements.txt`

### Quick Start

```sh
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload
```

### Running Tests

```sh
pytest -q
```

---

## Environment Variables

| Variable                        | Description                                                      | Default         |
|----------------------------------|------------------------------------------------------------------|-----------------|
| EXAMPLE_FASTAPI_DB               | Path to DuckDB file                                              | data.db         |
| DB_BACKEND                       | Database backend: 'duckdb' or 'snowflake'                        | duckdb          |
| EXAMPLE_FASTAPI_MAX_WORKERS      | Number of DB thread pool workers                                 | 100             |
| EXAMPLE_FASTAPI_READY_RETRIES    | Readiness check retries                                          | 3               |
| EXAMPLE_FASTAPI_READY_DELAY      | Delay between readiness retries (seconds)                        | 0.1             |
| EXAMPLE_FASTAPI_READY_TIMEOUT    | Readiness check timeout (seconds)                                | 1.0             |
| SNOWFLAKE_USER                   | Snowflake username                                               |                 |
| SNOWFLAKE_PASSWORD               | Password for Snowflake (password login)                          |                 |
| SNOWFLAKE_ACCOUNT                | Snowflake account identifier                                     |                 |
| SNOWFLAKE_DATABASE               | Snowflake database name                                          |                 |
| SNOWFLAKE_SCHEMA                 | Snowflake schema name                                            | PUBLIC          |
| SNOWFLAKE_WAREHOUSE              | Snowflake warehouse name                                         |                 |
| SNOWFLAKE_PRIVATE_KEY_PATH       | Path to private key file (keypair login)                         |                 |
| SNOWFLAKE_OAUTH_TOKEN            | OAuth2.0 token (oauth login)                                     |                 |
| LOGIN_TYPE                       | Snowflake login type: 'password', 'keypair', or 'oauth'          | password        |

---

## API Endpoints

### Health & Readiness

- `GET /health`
  **Response:**
  ```json
  { "status": "ok" }
  ```

- `GET /ready`
  **Response:**
  - Ready:
    ```json
    { "status": "ready" }
    ```
  - Unavailable:
    ```json
    { "status": "unavailable" }
    ```

### Item CRUD

All item endpoints accept and return dynamic fields. Example field set: `{ "name": "Widget", "value": "42", "color": "red" }`.

- `GET /items/`
  **Response:**
  ```json
  [
    { "id": 1, "name": "Widget", "value": "42", "color": "red" },
    { "id": 2, "name": "Gadget", "value": "99", "size": "large" }
  ]
  ```

- `GET /items/{item_id}`
  **Response:**
  ```json
  { "id": 1, "name": "Widget", "value": "42", "color": "red" }
  ```

- `POST /items/`
  **Request:**
  ```json
  { "name": "Widget", "value": "42", "color": "red" }
  ```
  **Response:**
  ```json
  { "id": 3, "name": "Widget", "value": "42", "color": "red" }
  ```

- `PUT /items/{item_id}`
  **Request:**
  ```json
  { "name": "Widget", "value": "43", "color": "blue" }
  ```
  **Response:**
  ```json
  { "id": 3, "name": "Widget", "value": "43", "color": "blue" }
  ```

- `DELETE /items/{item_id}`
  **Response:**
  - Success: HTTP 204 No Content
  - Not found:
    ```json
    { "error": "Item not found" }
    ```

---

## Error Handling

- All database errors are returned as JSON:
  ```json
  { "error": "Detailed error message" }
  ```
- Errors are also logged to `app_error.log` for backend diagnostics.

---

## Backend Selection

- Set `DB_BACKEND=duckdb` for local DuckDB.
- Set `DB_BACKEND=snowflake` for Snowflake; configure Snowflake env vars as needed.

---

## Snowflake Authentication

- `LOGIN_TYPE=password`: Uses `SNOWFLAKE_USER` and `SNOWFLAKE_PASSWORD`.
- `LOGIN_TYPE=keypair`: Uses `SNOWFLAKE_PRIVATE_KEY_PATH` for private key authentication.
- `LOGIN_TYPE=oauth`: Uses `SNOWFLAKE_OAUTH_TOKEN` for OAuth2.0 authentication.

---

## Advanced Usage

- You can POST/PUT any JSON object to `/items/` and `/items/{item_id}`; the backend will store all fields.
- The response will always include all fields stored for each item, including the auto-generated `id`.

---

## Example Client Requests

### Create Item

```http
POST /items/
Content-Type: application/json

{
  "name": "Widget",
  "value": "42",
  "color": "red"
}
```

### Update Item

```http
PUT /items/3
Content-Type: application/json

{
  "name": "Widget",
  "value": "43",
  "color": "blue"
}
```

### Error Response Example

```json
{ "error": "Constraint Error: Duplicate key violates primary key constraint." }
```

---

## Example Python Client Code

```python
import requests

# Create an item
resp = requests.post("http://localhost:8000/items/", json={"name": "Widget", "value": "42", "color": "red"})
print(resp.json())

# Update an item
resp = requests.put("http://localhost:8000/items/1", json={"name": "Widget", "value": "43", "color": "blue"})
print(resp.json())

# Get all items
resp = requests.get("http://localhost:8000/items/")
print(resp.json())

# Get a single item
resp = requests.get("http://localhost:8000/items/1")
print(resp.json())

# Delete an item
resp = requests.delete("http://localhost:8000/items/1")
print(resp.status_code)
```

---

## Example JavaScript Client Code

```javascript
fetch("/items/", {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify({ name: "Widget", value: "42", color: "red" })
})
  .then(resp => resp.json())
  .then(data => console.log(data));

fetch("/items/1", {
  method: "PUT",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify({ name: "Widget", value: "43", color: "blue" })
})
  .then(resp => resp.json())
  .then(data => console.log(data));
```

---

## Logging

- All errors and important events are logged to `app_error.log` and the console.
- Example log entry:
  ```
  2025-09-22 12:34:56 ERROR DB error in create_item: Constraint Error: Duplicate key violates primary key constraint.
  ```

---

## Project Structure

```
example-fastapi/
├── app/
│   ├── db.py
│   ├── db_snowflake.py
│   ├── health.py
│   ├── main.py
│   ├── routes.py
│   ├── schemas.py
├── requirements.txt
├── README.md
├── project_documentation.md
└── tests/
    ├── test_concurrency.py
    ├── test_ready.py
```

---

## Contributing

1. Fork the repository on GitHub.
2. Clone your fork and create a new branch.
3. Make your changes and commit with clear messages.
4. Push your branch and open a pull request.

---

## License

This project is licensed under the MIT License.

---

## Additional Code Samples

### FastAPI Endpoint Example (Python)

```python
from fastapi import FastAPI, Request
from app.db import db

app = FastAPI()

@app.post("/items/")
async def create_item(request: Request):
    data = await request.json()
    return await db.create_item(**data)

@app.get("/items/{item_id}")
async def get_item(item_id: int):
    return await db.fetch_item(item_id)
```

### Curl Examples

#### Create an Item
```sh
curl -X POST http://localhost:8000/items/ \
  -H "Content-Type: application/json" \
  -d '{"name": "Widget", "value": "42", "color": "red"}'
```

#### Update an Item
```sh
curl -X PUT http://localhost:8000/items/1 \
  -H "Content-Type: application/json" \
  -d '{"name": "Widget", "value": "43", "color": "blue"}'
```

#### Get All Items
```sh
curl http://localhost:8000/items/
```

#### Get Single Item
```sh
curl http://localhost:8000/items/1
```

#### Delete an Item
```sh
curl -X DELETE http://localhost:8000/items/1
```

### Error Handling Example (Python)

```python
from fastapi import HTTPException

try:
    # some db operation
    pass
except Exception as e:
    raise HTTPException(status_code=500, detail={"error": str(e)})
```

### Dynamic Field Example

You can POST any JSON object to `/items/`:

```json
{
  "name": "Widget",
  "value": "42",
  "color": "red",
  "size": "large",
  "tags": ["new", "sale"]
}
```

The response will include all fields:

```json
{
  "id": 5,
  "name": "Widget",
  "value": "42",
  "color": "red",
  "size": "large",
  "tags": ["new", "sale"]
}
```

### Snowflake Connection Example (Python)

```python
import snowflake.connector

conn = snowflake.connector.connect(
    user="$SNOWFLAKE_USER",
    password="$SNOWFLAKE_PASSWORD",
    account="$SNOWFLAKE_ACCOUNT",
    database="$SNOWFLAKE_DATABASE",
    schema="$SNOWFLAKE_SCHEMA",
    warehouse="$SNOWFLAKE_WAREHOUSE"
)
cur = conn.cursor()
cur.execute("SELECT CURRENT_VERSION()")
print(cur.fetchone())
cur.close()
conn.close()
```

---

For more advanced usage, see the main documentation above. If you need more code samples in other languages or for specific frameworks, let me know!
