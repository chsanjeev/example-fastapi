"""DuckDB-backed DB manager used by the FastAPI app.
DuckDB-backed DB manager used by the FastAPI app.



Attributes:
    DB_PATH (str): Path to the DuckDB database file, configurable via the EXAMPLE_FASTAPI_DB environment variable.
    MAX_WORKERS (int): Maximum number of worker threads for concurrent execution, configurable via the EXAMPLE_FASTAPI_MAX_WORKERS environment variable.
    db (DBManager): Singleton instance of DBManager for use throughout the application.

This file is written as a single atomic unit to avoid duplication issues.
"""

import asyncio
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional

import duckdb  # type: ignore

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", handlers=[logging.FileHandler("app_error.log", mode="a"), logging.StreamHandler()])


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

DB_PATH = os.getenv("EXAMPLE_FASTAPI_DB", "data.db")
MAX_WORKERS = int(os.getenv("EXAMPLE_FASTAPI_MAX_WORKERS", "100"))


class DBManager:

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
                value TEXT,
                number INTEGER
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
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._executor, lambda: fn(*args, **kwargs))

    def _fetch_all_items_with_where_sync(self, table_name: str, columns: Optional[List[str]] = None, order_by: str = "id", where: str = "") -> List[Dict[str, Any]]:
        """
        Fetches all items from the specified table with optional filtering, column selection, and ordering.

        Args:
            table_name (str): The name of the database table to query.
            columns (Optional[List[str]], optional): List of column names to select. If None, selects all columns. Defaults to None.
            order_by (str, optional): Column name to order the results by. Defaults to "id".
            where (str, optional): SQL WHERE clause to filter the results (without the 'WHERE' keyword). Defaults to "".

        Returns:
            List[Dict[str, Any]]: A list of dictionaries, each representing a row from the query result, with column names as keys.
        """
        conn = self._get_conn()
        where_clause = f" WHERE {where}" if where else ""
        if columns is None:
            sql: str = f"SELECT * FROM {table_name}{where_clause} ORDER BY {order_by}"
            logger.info("Executing SQL: %s", sql)
            cur = conn.execute(sql)
            rows = cur.fetchall()
            if cur.description is not None:
                columns = [desc[0] for desc in cur.description]
            else:
                columns = []
        else:
            col_str = ", ".join(columns)
            sql = f"SELECT {col_str} FROM {table_name}{where_clause} ORDER BY {order_by}"
            logger.info("Executing SQL: %s", sql)
            cur = conn.execute(sql)
            rows = cur.fetchall()
        return [dict(zip(columns, r)) for r in rows]

    async def fetch_all_items_with_where(self, table_name: str, columns: Optional[List[str]] = None, order_by: str = "id", where: str = "") -> List[Dict[str, Any]]:
        """
        This module provides the DBManager class for managing DuckDB database operations in a thread-safe and asynchronous manner.
        It initializes the database schema, manages connections using thread-local storage, and exposes CRUD operations for the 'items' table.
        Database operations are executed in a thread pool to avoid blocking the main event loop in asynchronous applications.

        Classes:
            DBManager: Handles connection management, schema initialization, and CRUD operations for the 'items' table.
        """
        return await self.run(self._fetch_all_items_with_where_sync, table_name, columns, order_by, where)

    def _fetch_all_items_sync(self, table_name: str, columns: Optional[List[str]] = None, order_by: str = "id") -> List[Dict[str, Any]]:
        """
        Retrieve all items from the database with a dynamic list of columns.

        Args:
            columns (Optional[List[str]]): List of column names to fetch. If None, fetches all columns.
            order_by (str): Column name to order the results by. Defaults to "id".

        Returns:
            List[Dict[str, Any]]: A list of dictionaries, each representing an item.
        """
        conn = self._get_conn()
        if columns is None:
            sql: str = f"SELECT * FROM {table_name} ORDER BY {order_by}"
            logger.info("Executing SQL: %s", sql)
            cur = conn.execute(sql)
            rows = cur.fetchall()
            if cur.description is not None:
                columns = [desc[0] for desc in cur.description]
            else:
                columns = []
        else:
            col_str = ", ".join(columns)
            sql = f"SELECT {col_str} FROM {table_name} ORDER BY {order_by}"
            logger.info("Executing SQL: %s", sql)
            cur = conn.execute(sql)
            rows = cur.fetchall()
        return [dict(zip(columns, r)) for r in rows]

    async def fetch_all_items(self, table_name: str, columns: Optional[List[str]] = None, order_by: str = "id") -> List[Dict[str, Any]]:
        """
        Asynchronously fetches all items from the database.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries, each representing an item retrieved from the database.

        Raises:
            Any exceptions raised by the underlying synchronous fetch method or during asynchronous execution.
        """
        return await self.run(self._fetch_all_items_sync, table_name, columns, order_by)

    def _fetch_item_with_where_sync(self, table_name: str, item_id: int, columns: Optional[List[str]] = None, order_by: str = "id", where: str = "") -> Optional[Dict[str, Any]]:
        """
        Fetches a single item from the database synchronously by its ID.

        Args:
            item_id (int): The unique identifier of the item to fetch.
            columns (Optional[List[str]], optional): A list of column names to retrieve.
                If None, all columns are fetched.

        Returns:
            Optional[Dict[str, Any]]: A dictionary mapping column names to their values for the fetched item,
                or None if the item does not exist.
        """
        conn = self._get_conn()
        where_clause = f" WHERE {where}" if where else ""
        if columns is None:
            sql: str = f"SELECT * FROM {table_name}{where_clause} ORDER BY {order_by}"
            if isinstance(item_id, int):
                sql = sql.replace(":item_id:", str(item_id))
            else:
                sql = sql.replace(":item_id:", item_id)
            logger.info("Executing SQL: %s", sql)
            cur = conn.execute(sql)
            row = cur.fetchone()
            if not row:
                return None
            if cur.description is not None:
                columns = [desc[0] for desc in cur.description]
            else:
                columns = []
        else:
            col_str = ", ".join(columns)
            sql = f"SELECT {col_str} FROM {table_name}{where_clause} ORDER BY {order_by}"
            if isinstance(item_id, int):
                sql = sql.replace(":item_id:", str(item_id))
            else:
                sql = sql.replace(":item_id:", item_id)
            logger.info("Executing SQL: %s", sql)
            cur = conn.execute(sql)
            row = cur.fetchone()
            if not row:
                return None

        return dict(zip(columns, row))

    async def fetch_item_with_where(self, table_name: str, item_id: int, columns: Optional[List[str]] = None, order_by: str = "id", where: str = "") -> Optional[Dict[str, Any]]:
        """
        Asynchronously fetches an item by its ID from the database.

        Args:
            item_id (int): The unique identifier of the item to fetch.

        Returns:
            Optional[Dict[str, Any]]: A dictionary representing the item if found,
            otherwise None.

        Raises:
            Exception: Propagates any exception raised during the database operation.
        """
        return await self.run(self._fetch_item_sync, table_name, item_id, columns, order_by, where)

    def _fetch_item_sync(self, table_name: str, item_id: int, columns: Optional[List[str]] = None, order_by: str = "id") -> Optional[Dict[str, Any]]:
        """
        Fetches a single item from the database synchronously by its ID.

        Args:
            item_id (int): The unique identifier of the item to fetch.
            columns (Optional[List[str]], optional): A list of column names to retrieve.
                If None, all columns are fetched.

        Returns:
            Optional[Dict[str, Any]]: A dictionary mapping column names to their values for the fetched item,
                or None if the item does not exist.
        """
        conn = self._get_conn()
        if columns is None:
            cur = conn.execute(f"SELECT * FROM {table_name} WHERE id = ? ORDER BY {order_by}", (item_id,))
            row = cur.fetchone()
            if not row:
                return None
            if cur.description is not None:
                columns = [desc[0] for desc in cur.description]
            else:
                columns = []
        else:
            col_str = ", ".join(columns)
            cur = conn.execute(f"SELECT {col_str} FROM {table_name} WHERE id = ? ORDER BY {order_by}", (item_id,))
            row = cur.fetchone()
            if not row:
                return None

        return dict(zip(columns, row))

    async def fetch_item(self, table_name: str, item_id: int, columns: Optional[List[str]] = None, order_by: str = "id") -> Optional[Dict[str, Any]]:
        """
        Asynchronously fetches an item by its ID from the database.

        Args:
            item_id (int): The unique identifier of the item to fetch.

        Returns:
            Optional[Dict[str, Any]]: A dictionary representing the item if found,
            otherwise None.

        Raises:
            Exception: Propagates any exception raised during the database operation.
        """
        return await self.run(self._fetch_item_sync, table_name, item_id, columns, order_by)

    def _create_item_sync(self, table_name: str, **fields) -> Dict[str, Any]:
        """
        Synchronously creates a new item in the 'items' table with the provided fields.

        This method generates a new unique item ID using the 'items_seq' sequence,
        constructs an SQL INSERT statement with the given fields, and inserts the new item
        into the database. The method returns a dictionary containing the newly created item's
        ID and the provided fields.

        Args:
            **fields: Arbitrary keyword arguments representing column names and their values
                      to be inserted into the 'items' table.

        Returns:
            Dict[str, Any]: A dictionary containing the 'id' of the newly created item and
                            all provided fields.

        Raises:
            RuntimeError: If the next item ID cannot be generated from the sequence.
        """
        conn = self._get_conn()
        seq_row = conn.execute("SELECT nextval('items_seq')").fetchone()
        if seq_row is None:
            raise RuntimeError("Failed to generate next item id from sequence.")
        item_id = int(seq_row[0])
        columns = ["id"] + list(fields.keys())
        values = [item_id] + list(fields.values())
        placeholders = ", ".join(["?" for _ in columns])
        sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
        logger.info("Executing SQL: %s", sql)
        conn.execute(sql, values)
        result = {"id": item_id}
        result.update(fields)
        return result

    async def create_item(self, table_name: str, **fields) -> Dict[str, Any]:
        """
        Asynchronously creates a new item in the database with the specified fields.

        Args:
            **fields: Arbitrary keyword arguments representing the fields and their values for the new item.

        Returns:
            Dict[str, Any]: A dictionary containing the details of the created item.

        Raises:
            Exception: If item creation fails due to database errors or invalid input.
        """
        return await self.run(self._create_item_sync, table_name, **fields)

    def _update_item_sync(self, table_name: str, item_id: int, **fields) -> Optional[Dict[str, Any]]:
        conn = self._get_conn()
        set_clause = ", ".join([f"{k} = ?" for k in fields.keys()])
        values = list(fields.values()) + [item_id]
        sql = f"UPDATE {table_name} SET {set_clause} WHERE id = ?"
        logger.info("Executing SQL: %s", sql)
        conn.execute(sql, values)
        cur = conn.execute("SELECT changes()")
        row = cur.fetchone()
        changed = int(row[0]) if row is not None else 0
        if changed == 0:
            return None
        result = {"id": item_id}
        result.update(fields)
        return result

    async def update_item(self, table_name: str, item_id: int, **fields) -> Optional[Dict[str, Any]]:
        """
        Asynchronously updates an item in the database with the specified fields.

        Args:
            item_id (int): The unique identifier of the item to update.
            **fields: Arbitrary keyword arguments representing the fields to update and their new values.

        Returns:
            Optional[Dict[str, Any]]: A dictionary containing the updated item data if the update was successful,
            or None if the item was not found or the update failed.

        Raises:
            Exception: If an error occurs during the update operation.
        """
        return await self.run(self._update_item_sync, table_name, item_id, **fields)

    def _delete_item_sync(self, table_name: str, item_id: int) -> bool:
        """
        This module provides database utility functions for managing items in the application.
        It includes synchronous and asynchronous operations for CRUD actions on the items table.
        """
        conn = self._get_conn()
        conn.execute(f"DELETE FROM {table_name} WHERE id = ?", (item_id,))
        cur = conn.execute("SELECT changes()")
        row = cur.fetchone()
        changed = int(row[0]) if row is not None else 0
        return changed > 0

    async def delete_item(self, table_name: str, item_id: int) -> bool:
        """
        Asynchronously deletes an item from the database by its ID.

        Args:
            item_id (int): The unique identifier of the item to be deleted.

        Returns:
            bool: True if the item was successfully deleted, False otherwise.

        Raises:
            Exception: If the deletion operation fails.
        """
        return await self.run(self._delete_item_sync, table_name, item_id)

    def check_connection(self, retries: int = 3, delay: float = 0.1) -> bool:
        """Run a quick sync connectivity check with retries.

        This method is synchronous and intended to be executed inside the
        DBManager's threadpool (via `await db.run(db.check_connection, ...)`).
        """

        for attempt in range(retries):
            try:
                conn = self._get_conn()
                cur = conn.execute("SELECT 1")
                row = cur.fetchone()
                if row and row[0] == 1:
                    return True
            except duckdb.Error:
                # swallow and retry
                pass
            if attempt + 1 < retries:
                time.sleep(delay)
        return False

    def close(self):
        """
        Shuts down the internal executor, waiting for all running tasks to complete.

        This method should be called to gracefully terminate the executor and release resources
        when the database connection is no longer needed.
        """
        self._executor.shutdown(wait=True)


# singleton
db = DBManager()
