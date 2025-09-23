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

import asyncio
import logging
import os
import threading
import time
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
    """
    DBManager provides asynchronous and synchronous methods for interacting with a Snowflake database.

    This class manages database connections, executes queries, and performs CRUD operations on tables.
    It supports multiple authentication methods (password, keypair, OAuth) and uses a thread pool executor
    to run blocking database operations asynchronously.

    Attributes:
        _executor (ThreadPoolExecutor): Thread pool for running sync DB operations asynchronously.
        _local (threading.local): Thread-local storage for per-thread DB connections.

    Methods:
        _get_conn():
            Retrieves or creates a thread-local Snowflake database connection using configured authentication.

        run(fn, *args, **kwargs):
            Asynchronously executes a synchronous function using the thread pool executor.

        _fetch_all_items_sync(table_name, columns=None, order_by="id"):
            Synchronously fetches all rows from a table, optionally selecting columns and ordering.

        fetch_all_items(table_name, columns=None, order_by="id"):
            Asynchronously fetches all rows from a table.

        _fetch_all_items_with_where_sync(table_name, columns=None, order_by="id", where=""):
            Synchronously fetches rows from a table with optional filtering and ordering.

        fetch_all_items_with_where(table_name, columns=None, order_by="id", where=""):
            Asynchronously fetches rows from a table with optional filtering and ordering.

        _fetch_item_sync(table_name, item_id, columns=None, order_by="id"):
            Synchronously fetches a single row by ID from a table.

        fetch_item(table_name, item_id, columns=None, order_by="id"):
            Asynchronously fetches a single row by ID from a table.

        _create_item_sync(table_name, **fields):
            Synchronously inserts a new row into a table and returns the inserted item.

        create_item(table_name, **fields):
            Asynchronously inserts a new row into a table.

        _update_item_sync(table_name, item_id, **fields):
            Synchronously updates a row by ID in a table and returns the updated item.

        update_item(table_name, item_id, **fields):
            Asynchronously updates a row by ID in a table.

        _delete_item_sync(table_name, item_id):
            Synchronously deletes a row by ID from a table.

        delete_item(table_name, item_id):
            Asynchronously deletes a row by ID from a table.

        check_connection(retries=3, delay=0.1):
            Checks database connectivity by executing a simple query, with retries.

        close():
            Shuts down the thread pool executor, waiting for all tasks to complete.

    Usage:
        Instantiate DBManager and use its async methods within an async context (e.g., FastAPI endpoints)
        to interact with Snowflake tables in a non-blocking manner.
    """

    def __init__(self):
        self._executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
        self._local = threading.local()

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
                    conn = snowflake.connector.connect(
                        user=SNOWFLAKE_USER,
                        token=SNOWFLAKE_OAUTH_TOKEN,
                        account=SNOWFLAKE_ACCOUNT,
                        database=SNOWFLAKE_DATABASE,
                        schema=SNOWFLAKE_SCHEMA,
                        warehouse=SNOWFLAKE_WAREHOUSE,
                        authenticator="oauth",
                    )
                else:
                    raise RuntimeError(f"Unknown LOGIN_TYPE: {LOGIN_TYPE}")
                self._local.conn = conn
            except Exception as exc:
                logger.error("Failed to connect to Snowflake: %s", exc)
                raise
        return conn

    async def run(self, fn, *args, **kwargs):
        """
        Executes a synchronous function asynchronously using a thread pool executor.

        Args:
            fn (Callable): The function to execute.
            *args: Positional arguments to pass to the function.
            **kwargs: Keyword arguments to pass to the function.

        Returns:
            Any: The result returned by the executed function.

        Raises:
            Exception: Propagates any exception raised by the function.
        """
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._executor, lambda: fn(*args, **kwargs))

    def _fetch_all_items_sync(self, table_name: str, columns: Optional[List[str]] = None, order_by: str = "id") -> List[Dict[str, Any]]:
        """
        Fetches all items from the specified table in the database synchronously.

        Args:
            table_name (str): The name of the table to query.
            columns (Optional[List[str]], optional): List of column names to retrieve. If None, all columns are selected. Defaults to None.
            order_by (str, optional): The column name to order the results by. Defaults to "id".

        Returns:
            List[Dict[str, Any]]: A list of dictionaries, each representing a row from the table with column names as keys.

        Raises:
            Exception: If there is an error executing the SQL query or fetching results.

        Note:
            This method assumes that the table and columns exist in the database and that the connection is valid.
        """
        conn = self._get_conn()
        cur = conn.cursor()
        if columns is None:
            cur.execute(f"SELECT * FROM {table_name} ORDER BY {order_by}")
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description] if cur.description else []
        else:
            col_str = ", ".join(columns)
            cur.execute(f"SELECT {col_str} FROM {table_name} ORDER BY {order_by}")
            rows = cur.fetchall()
        cur.close()
        return [dict(zip(columns, r)) for r in rows]

    async def fetch_all_items(self, table_name: str, columns: Optional[List[str]] = None, order_by: str = "id") -> List[Dict[str, Any]]:
        """
        Asynchronously fetches all items from the specified table.

        Args:
            table_name (str): The name of the table to query.
            columns (Optional[List[str]], optional): A list of column names to retrieve. If None, all columns are fetched. Defaults to None.
            order_by (str, optional): The column name to order the results by. Defaults to "id".

        Returns:
            List[Dict[str, Any]]: A list of dictionaries, each representing a row from the table.

        Raises:
            Exception: If the query fails or the table does not exist.
        """
        return await self.run(self._fetch_all_items_sync, table_name, columns, order_by)

    def _fetch_all_items_with_where_sync(self, table_name: str, columns: Optional[List[str]] = None, order_by: str = "id", where: str = "") -> List[Dict[str, Any]]:
        """
        Fetches all items from the specified table with optional filtering, column selection, and ordering.

        Args:
            table_name (str): The name of the table to query.
            columns (Optional[List[str]], optional): List of column names to select. If None, selects all columns. Defaults to None.
            order_by (str, optional): Column name to order the results by. Defaults to "id".
            where (str, optional): SQL WHERE clause conditions (without the 'WHERE' keyword). If empty, no filtering is applied. Defaults to "".

        Returns:
            List[Dict[str, Any]]: A list of dictionaries, each representing a row from the query result, with column names as keys.
        """
        conn = self._get_conn()
        cur = conn.cursor()
        where_clause = f" WHERE {where}" if where else ""
        if columns is None:
            cur.execute(f"SELECT * FROM {table_name}{where_clause} ORDER BY {order_by}")
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description] if cur.description else []
        else:
            col_str = ", ".join(columns)
            cur.execute(f"SELECT {col_str} FROM {table_name}{where_clause} ORDER BY {order_by}")
            rows = cur.fetchall()
        cur.close()
        return [dict(zip(columns, r)) for r in rows]

    async def fetch_all_items_with_where(self, table_name: str, columns: Optional[List[str]] = None, order_by: str = "id", where: str = "") -> List[Dict[str, Any]]:
        """
        Asynchronously fetches all items from the specified table with optional filtering and ordering.

        Args:
            table_name (str): The name of the table to query.
            columns (Optional[List[str]], optional): List of column names to retrieve. If None, all columns are selected. Defaults to None.
            order_by (str, optional): Column name to order the results by. Defaults to "id".
            where (str, optional): SQL WHERE clause to filter the results. Defaults to an empty string (no filtering).

        Returns:
            List[Dict[str, Any]]: A list of dictionaries representing the rows fetched from the table.

        """
        return await self.run(self._fetch_all_items_with_where_sync, table_name, columns, order_by, where)

    def _fetch_item_sync(self, table_name: str, item_id: int, columns: Optional[List[str]] = None, order_by: str = "id") -> Optional[Dict[str, Any]]:
        """
        Fetches a single item from the specified table synchronously by its ID.

        Args:
            table_name (str): The name of the table to query.
            item_id (int): The ID of the item to fetch.
            columns (Optional[List[str]], optional): List of column names to retrieve. If None, all columns are fetched. Defaults to None.
            order_by (str, optional): Column name to order the results by. Defaults to "id".

        Returns:
            Optional[Dict[str, Any]]: A dictionary mapping column names to their values for the fetched item,
            or None if no item is found.
        """
        conn = self._get_conn()
        cur = conn.cursor()
        if columns is None:
            cur.execute(f"SELECT * FROM {table_name} WHERE id = %s ORDER BY {order_by}", (item_id,))
            row = cur.fetchone()
            columns = [desc[0] for desc in cur.description] if cur.description else []
        else:
            col_str = ", ".join(columns)
            cur.execute(f"SELECT {col_str} FROM {table_name} WHERE id = %s ORDER BY {order_by}", (item_id,))
            row = cur.fetchone()
        cur.close()
        if not row:
            return None
        return dict(zip(columns, row))

    async def fetch_item(self, table_name: str, item_id: int, columns: Optional[List[str]] = None, order_by: str = "id") -> Optional[Dict[str, Any]]:
        """
        Asynchronously fetches a single item from the specified table by its ID.

        Args:
            table_name (str): The name of the table to query.
            item_id (int): The ID of the item to fetch.
            columns (Optional[List[str]], optional): List of column names to retrieve. If None, all columns are fetched. Defaults to None.
            order_by (str, optional): Column name to order the results by. Defaults to "id".

        Returns:
            Optional[Dict[str, Any]]: A dictionary representing the fetched item if found, otherwise None.
        """
        return await self.run(self._fetch_item_sync, table_name, item_id, columns, order_by)

    def _create_item_sync(self, table_name: str, **fields) -> Dict[str, Any]:
        """
        Inserts a new item into the specified table synchronously.

        Args:
            table_name (str): The name of the table to insert the item into.
            **fields: Arbitrary keyword arguments representing column names and their corresponding values to insert.

        Returns:
            Dict[str, Any]: A dictionary containing the inserted item's ID under the key 'id', along with the provided fields.

        Note:
            - Assumes the underlying database supports LAST_INSERT_ID() for retrieving the last inserted row's ID.
            - The connection and cursor are managed within the method and closed after execution.
        """
        conn = self._get_conn()
        cur = conn.cursor()
        columns = list(fields.keys())
        values = list(fields.values())
        placeholders = ", ".join(["%s" for _ in columns])
        sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
        cur.execute(sql, values)
        cur.execute("SELECT LAST_INSERT_ID()")
        row = cur.fetchone()
        item_id = int(row[0]) if row and row[0] is not None else None
        cur.close()
        result = {"id": item_id}
        result.update(fields)
        return result

    async def create_item(self, table_name: str, **fields) -> Dict[str, Any]:
        """
        Asynchronously creates a new item in the specified table with the given fields.

        Args:
            table_name (str): The name of the table where the item will be created.
            **fields: Arbitrary keyword arguments representing the fields and their values for the new item.

        Returns:
            Dict[str, Any]: A dictionary containing the details of the created item.

        Raises:
            Exception: If the item creation fails or an error occurs during execution.
        """
        return await self.run(self._create_item_sync, table_name, **fields)

    def _update_item_sync(self, table_name: str, item_id: int, **fields) -> Optional[Dict[str, Any]]:
        """
        Updates an item in the specified table synchronously with the provided fields.

        Args:
            table_name (str): The name of the table to update.
            item_id (int): The ID of the item to update.
            **fields: Arbitrary keyword arguments representing the fields to update and their new values.

        Returns:
            Optional[Dict[str, Any]]: A dictionary containing the updated fields and item ID if the update was successful,
            or None if no rows were changed.

        Raises:
            Exception: If there is an error executing the SQL statement or connecting to the database.

        Note:
            This method assumes the table has an 'id' column as the primary key.
        """
        conn = self._get_conn()
        cur = conn.cursor()
        set_clause = ", ".join([f"{k} = %s" for k in fields.keys()])
        values = list(fields.values()) + [item_id]
        sql = f"UPDATE {table_name} SET {set_clause} WHERE id = %s"
        cur.execute(sql, values)
        cur.execute("SELECT ROW_COUNT()")
        row = cur.fetchone()
        changed = int(row[0]) if row and row[0] is not None else 0
        cur.close()
        if changed == 0:
            return None
        result = {"id": item_id}
        result.update(fields)
        return result

    async def update_item(self, table_name: str, item_id: int, **fields) -> Optional[Dict[str, Any]]:
        """
        Asynchronously updates an item in the specified table with the given fields.

        Args:
            table_name (str): The name of the table where the item resides.
            item_id (int): The unique identifier of the item to update.
            **fields: Arbitrary keyword arguments representing the fields to update and their new values.

        Returns:
            Optional[Dict[str, Any]]: The updated item as a dictionary if the update is successful, otherwise None.

        Raises:
            Exception: If the update operation fails.
        """
        return await self.run(self._update_item_sync, table_name, item_id, **fields)

    def _delete_item_sync(self, table_name: str, item_id: int) -> bool:
        """
        Deletes an item from the specified table by its ID in a synchronous manner.

        Args:
            table_name (str): The name of the table from which to delete the item.
            item_id (int): The ID of the item to be deleted.

        Returns:
            bool: True if an item was deleted (i.e., the row count changed), False otherwise.

        Raises:
            Exception: If there is an error during the database operation.
        """
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute(f"DELETE FROM {table_name} WHERE id = %s", (item_id,))
        cur.execute("SELECT ROW_COUNT()")
        row = cur.fetchone()
        changed = int(row[0]) if row and row[0] is not None else 0
        cur.close()
        return changed > 0

    async def delete_item(self, table_name: str, item_id: int) -> bool:
        """
        Asynchronously deletes an item from the specified table by its ID.

        Args:
            table_name (str): The name of the table from which to delete the item.
            item_id (int): The unique identifier of the item to be deleted.

        Returns:
            bool: True if the item was successfully deleted, False otherwise.

        Raises:
            Exception: If the deletion operation fails.
        """
        return await self.run(self._delete_item_sync, table_name, item_id)

    def check_connection(self, retries: int = 3, delay: float = 0.1) -> bool:
        """
        Checks the database connection by executing a simple query.

        Attempts to establish a connection and execute "SELECT 1" up to a specified number of retries.
        If the query returns 1, the connection is considered successful.

        Args:
            retries (int, optional): Number of times to retry the connection. Defaults to 3.
            delay (float, optional): Delay in seconds between retries. Defaults to 0.1.

        Returns:
            bool: True if the connection is successful, False otherwise.
        """

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
        """
        Shuts down the executor, waiting for all running tasks to complete before closing.

        This method ensures that all background operations are finished before releasing resources.
        """
        self._executor.shutdown(wait=True)


# singleton
db = DBManager()
