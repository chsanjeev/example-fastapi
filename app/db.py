"""DuckDB-backed DB manager used by the FastAPI app.
DuckDB-backed DB manager used by the FastAPI app.



Attributes:
    DB_PATH (str): Path to the DuckDB database file, configurable via the EXAMPLE_FASTAPI_DB environment variable.
    MAX_WORKERS (int): Maximum number of worker threads for concurrent execution, configurable via the EXAMPLE_FASTAPI_MAX_WORKERS environment variable.
    db (DBManager): Singleton instance of DBManager for use throughout the application.

This file is written as a single atomic unit to avoid duplication issues.
"""

import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional

import duckdb  # type: ignore

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", handlers=[logging.FileHandler("app_error.log", mode="a"), logging.StreamHandler()])


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

DB_PATH = os.getenv("EXAMPLE_FASTAPI_DB", "data.db")
MAX_WORKERS = int(os.getenv("EXAMPLE_FASTAPI_MAX_WORKERS", "100"))


class DBManager:
    """
    This module provides the DBManager class for managing DuckDB database operations in a thread-safe and asynchronous manner.
    It initializes the database schema, manages connections using thread-local storage, and exposes CRUD operations for the 'items' table.
    Database operations are executed in a thread pool to avoid blocking the main event loop in asynchronous applications.

    Classes:
        DBManager: Handles connection management, schema initialization, and CRUD operations for the 'items' table.
    """

    def __init__(self, db_path: str = DB_PATH, max_workers: int = MAX_WORKERS):
        """
        Module for database initialization and connection management using DuckDB.

        This module provides a class for handling database connections, schema initialization,
        and thread-safe execution using a thread pool. The database schema includes a sequence
        and a table for storing items with unique IDs, names, and values.

            Initialize the database connection and schema.

            Args:
                db_path (str): Path to the DuckDB database file. Defaults to DB_PATH.
                max_workers (int): Maximum number of worker threads for concurrent execution. Defaults to MAX_WORKERS.

            Initializes a thread pool executor and thread-local storage for database connections.
            Ensures the database schema is created, including a sequence for item IDs and an 'items' table.
        """
        self.db_path = db_path
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._local = threading.local()

        # initialize DB schema (safe to call multiple times)
        conn = duckdb.connect(self.db_path)
        conn.execute("CREATE SEQUENCE IF NOT EXISTS items_seq START 1")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS items (
                id INTEGER DEFAULT nextval('items_seq') PRIMARY KEY,
                name TEXT NOT NULL,
                value TEXT
            )
            """
        )
        conn.close()

    def _get_conn(self) -> duckdb.DuckDBPyConnection:
        """
        This module provides database connection management utilities for DuckDB.

        Functions:
            _get_conn: Retrieves a thread-local DuckDB connection, creating one if it does not exist.

        Retrieves a thread-local DuckDB connection. If a connection does not already exist for the current thread,
        a new one is created using the specified database path and stored in the thread-local storage.

        Returns:
            duckdb.DuckDBPyConnection: The DuckDB connection associated with the current thread.
        """
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = duckdb.connect(self.db_path)
            self._local.conn = conn
        return conn

    async def run(self, fn, *args, **kwargs):
        """
        This module provides asynchronous database utilities.

        Functions:
            run(fn, *args, **kwargs): Executes a synchronous function asynchronously using an executor.
        """
        import asyncio

        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._executor, lambda: fn(*args, **kwargs))

    # CRUD
    def _fetch_all_items_sync(self) -> List[Dict[str, Any]]:
        """
        Module for database operations related to item retrieval.

        This module provides synchronous methods for fetching item records from the database.
        Retrieve all items from the database in a synchronous manner.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries, each representing an item with
                'id' (int), 'name' (str), and 'value' (Any) fields.
        """
        conn = self._get_conn()
        cur = conn.execute("SELECT id, name, value FROM items ORDER BY id")
        rows = cur.fetchall()
        return [{"id": int(r[0]), "name": r[1], "value": r[2]} for r in rows]

    async def fetch_all_items(self) -> List[Dict[str, Any]]:
        return await self.run(self._fetch_all_items_sync)

    def _fetch_item_sync(self, item_id: int) -> Optional[Dict[str, Any]]:
        conn = self._get_conn()
        cur = conn.execute("SELECT id, name, value FROM items WHERE id = ?", (item_id,))
        row = cur.fetchone()
        if not row:
            return None
        return {"id": int(row[0]), "name": row[1], "value": row[2]}

    async def fetch_item(self, item_id: int) -> Optional[Dict[str, Any]]:
        return await self.run(self._fetch_item_sync, item_id)

    def _create_item_sync(self, **fields) -> Dict[str, Any]:
        conn = self._get_conn()
        seq_row = conn.execute("SELECT nextval('items_seq')").fetchone()
        item_id = int(seq_row[0])
        columns = ["id"] + list(fields.keys())
        values = [item_id] + list(fields.values())
        placeholders = ", ".join(["?" for _ in columns])
        sql = f"INSERT INTO items ({', '.join(columns)}) VALUES ({placeholders})"
        conn.execute(sql, values)
        result = {"id": item_id}
        result.update(fields)
        return result

    async def create_item(self, **fields) -> Dict[str, Any]:
        return await self.run(self._create_item_sync, **fields)

    def _update_item_sync(self, item_id: int, **fields) -> Optional[Dict[str, Any]]:
        conn = self._get_conn()
        set_clause = ", ".join([f"{k} = ?" for k in fields.keys()])
        values = list(fields.values()) + [item_id]
        sql = f"UPDATE items SET {set_clause} WHERE id = ?"
        conn.execute(sql, values)
        cur = conn.execute("SELECT changes()")
        changed = int(cur.fetchone()[0])
        if changed == 0:
            return None
        result = {"id": item_id}
        result.update(fields)
        return result

    async def update_item(self, item_id: int, **fields) -> Optional[Dict[str, Any]]:
        return await self.run(self._update_item_sync, item_id, **fields)

    def _delete_item_sync(self, item_id: int) -> bool:
        """
        This module provides database utility functions for managing items in the application.
        It includes synchronous and asynchronous operations for CRUD actions on the items table.
        """
        conn = self._get_conn()
        conn.execute("DELETE FROM items WHERE id = ?", (item_id,))
        cur = conn.execute("SELECT changes()")
        changed = int(cur.fetchone()[0])
        return changed > 0

    async def delete_item(self, item_id: int) -> bool:
        return await self.run(self._delete_item_sync, item_id)

    def check_connection(self, retries: int = 3, delay: float = 0.1) -> bool:
        """Run a quick sync connectivity check with retries.

        This method is synchronous and intended to be executed inside the
        DBManager's threadpool (via `await db.run(db.check_connection, ...)`).
        """
        import time

        for attempt in range(retries):
            try:
                conn = self._get_conn()
                cur = conn.execute("SELECT 1")
                row = cur.fetchone()
                if row and row[0] == 1:
                    return True
            except Exception:
                # swallow and retry
                pass
            if attempt + 1 < retries:
                time.sleep(delay)
        return False

    def close(self):
        self._executor.shutdown(wait=True)


# singleton
db = DBManager()
