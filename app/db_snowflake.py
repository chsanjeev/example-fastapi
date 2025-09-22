"""
Snowflake-backed DB manager for FastAPI app.

Supports three login types for Snowflake connection:
    - password: Username/password authentication (default)
    - keypair: Private key authentication
    - oauth: OAuth2.0 token authentication

Login type is selected via the LOGIN_TYPE environment variable (default: password).

Required environment variables:
    SNOWFLAKE_USER
    SNOWFLAKE_ACCOUNT
    SNOWFLAKE_DATABASE
    SNOWFLAKE_SCHEMA (default: PUBLIC)
    SNOWFLAKE_WAREHOUSE
    SNOWFLAKE_PASSWORD (for password login)
    SNOWFLAKE_PRIVATE_KEY_PATH (for keypair login)
    SNOWFLAKE_OAUTH_TOKEN (for oauth login)
    LOGIN_TYPE (password, keypair, or oauth)

Example:
    export LOGIN_TYPE=keypair
    export SNOWFLAKE_PRIVATE_KEY_PATH=/path/to/pk.pem
"""

import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional

import snowflake.connector

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", handlers=[logging.FileHandler("app_error.log", mode="a"), logging.StreamHandler()])
logger = logging.getLogger(__name__)

LOGIN_TYPE = os.getenv("LOGIN_TYPE", "password")
SNOWFLAKE_PRIVATE_KEY_PATH = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
SNOWFLAKE_OAUTH_TOKEN = os.getenv("SNOWFLAKE_OAUTH_TOKEN")

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Snowflake connection parameters from environment variables
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
MAX_WORKERS = int(os.getenv("EXAMPLE_FASTAPI_MAX_WORKERS", "10"))


class DBManager:
    def __init__(self):
        self._executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
        self._local = threading.local()
        self._init_schema()

    def _get_conn(self):
        conn = getattr(self._local, "conn", None)
        if conn is None:
            try:
                # Check required env vars
                missing = []
                for var in ["SNOWFLAKE_USER", "SNOWFLAKE_ACCOUNT", "SNOWFLAKE_DATABASE", "SNOWFLAKE_WAREHOUSE"]:
                    if not globals()[var]:
                        missing.append(var)
                if missing:
                    raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")
                if LOGIN_TYPE == "password":
                    if not SNOWFLAKE_PASSWORD:
                        raise RuntimeError("SNOWFLAKE_PASSWORD env var required for password login")
                    conn = snowflake.connector.connect(
                        user=SNOWFLAKE_USER,
                        password=SNOWFLAKE_PASSWORD,
                        account=SNOWFLAKE_ACCOUNT,
                        database=SNOWFLAKE_DATABASE,
                        schema=SNOWFLAKE_SCHEMA,
                        warehouse=SNOWFLAKE_WAREHOUSE,
                    )
                elif LOGIN_TYPE == "keypair":
                    if not SNOWFLAKE_PRIVATE_KEY_PATH:
                        raise RuntimeError("SNOWFLAKE_PRIVATE_KEY_PATH env var required for keypair login")
                    try:
                        with open(SNOWFLAKE_PRIVATE_KEY_PATH, "rb") as key:
                            pk = key.read()
                    except Exception as exc:
                        raise RuntimeError(f"Failed to read private key file: {exc}") from exc
                    conn = snowflake.connector.connect(
                        user=SNOWFLAKE_USER,
                        private_key=pk,
                        account=SNOWFLAKE_ACCOUNT,
                        database=SNOWFLAKE_DATABASE,
                        schema=SNOWFLAKE_SCHEMA,
                        warehouse=SNOWFLAKE_WAREHOUSE,
                    )
                elif LOGIN_TYPE == "oauth":
                    if not SNOWFLAKE_OAUTH_TOKEN:
                        raise RuntimeError("SNOWFLAKE_OAUTH_TOKEN env var required for oauth login")
                    conn = snowflake.connector.connect(user=SNOWFLAKE_USER, token=SNOWFLAKE_OAUTH_TOKEN, account=SNOWFLAKE_ACCOUNT, database=SNOWFLAKE_DATABASE, schema=SNOWFLAKE_SCHEMA, warehouse=SNOWFLAKE_WAREHOUSE, authenticator="oauth")
                else:
                    raise RuntimeError(f"Unknown LOGIN_TYPE: {LOGIN_TYPE}")
                self._local.conn = conn
            except Exception as exc:
                logger.error(f"Failed to connect to Snowflake: {exc}")
                raise
        return conn

    def _init_schema(self):
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
            warehouse=SNOWFLAKE_WAREHOUSE,
        )
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS items (
                id INTEGER AUTOINCREMENT PRIMARY KEY,
                name STRING NOT NULL,
                value STRING
            )
        """
        )
        cur.close()
        conn.close()

    async def run(self, fn, *args, **kwargs):
        import asyncio

        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._executor, lambda: fn(*args, **kwargs))

    def _fetch_all_items_sync(self) -> List[Dict[str, Any]]:
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute("SELECT id, name, value FROM items ORDER BY id")
        rows = cur.fetchall()
        cur.close()
        return [{"id": int(r[0]), "name": r[1], "value": r[2]} for r in rows]

    async def fetch_all_items(self) -> List[Dict[str, Any]]:
        return await self.run(self._fetch_all_items_sync)

    def _fetch_item_sync(self, item_id: int) -> Optional[Dict[str, Any]]:
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute("SELECT id, name, value FROM items WHERE id = %s", (item_id,))
        row = cur.fetchone()
        cur.close()
        if not row:
            return None
        return {"id": int(row[0]), "name": row[1], "value": row[2]}

    async def fetch_item(self, item_id: int) -> Optional[Dict[str, Any]]:
        return await self.run(self._fetch_item_sync, item_id)

    def _create_item_sync(self, **fields) -> Dict[str, Any]:
        conn = self._get_conn()
        cur = conn.cursor()
        columns = list(fields.keys())
        values = list(fields.values())
        placeholders = ", ".join(["%s" for _ in columns])
        sql = f"INSERT INTO items ({', '.join(columns)}) VALUES ({placeholders})"
        cur.execute(sql, values)
        cur.execute("SELECT LAST_INSERT_ID()")
        item_id = int(cur.fetchone()[0])
        cur.close()
        result = {"id": item_id}
        result.update(fields)
        return result

    async def create_item(self, **fields) -> Dict[str, Any]:
        return await self.run(self._create_item_sync, **fields)

    def _update_item_sync(self, item_id: int, **fields) -> Optional[Dict[str, Any]]:
        conn = self._get_conn()
        cur = conn.cursor()
        set_clause = ", ".join([f"{k} = %s" for k in fields.keys()])
        values = list(fields.values()) + [item_id]
        sql = f"UPDATE items SET {set_clause} WHERE id = %s"
        cur.execute(sql, values)
        cur.execute("SELECT ROW_COUNT()")
        changed = int(cur.fetchone()[0])
        cur.close()
        if changed == 0:
            return None
        result = {"id": item_id}
        result.update(fields)
        return result

    async def update_item(self, item_id: int, **fields) -> Optional[Dict[str, Any]]:
        return await self.run(self._update_item_sync, item_id, **fields)

    def _delete_item_sync(self, item_id: int) -> bool:
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute("DELETE FROM items WHERE id = %s", (item_id,))
        cur.execute("SELECT ROW_COUNT()")
        changed = int(cur.fetchone()[0])
        cur.close()
        return changed > 0

    async def delete_item(self, item_id: int) -> bool:
        return await self.run(self._delete_item_sync, item_id)

    def check_connection(self, retries: int = 3, delay: float = 0.1) -> bool:
        import time

        for attempt in range(retries):
            try:
                conn = self._get_conn()
                cur = conn.cursor()
                cur.execute("SELECT 1")
                row = cur.fetchone()
                cur.close()
                if row and row[0] == 1:
                    return True
            except Exception:
                pass
            if attempt + 1 < retries:
                time.sleep(delay)
        return False

    def close(self):
        self._executor.shutdown(wait=True)


# singleton
db = DBManager()
