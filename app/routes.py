"""
This module defines the FastAPI API routes for item operations.

    - GET /items/: Retrieves a list of items from the database.
    - GET /items/{item_id}: Retrieves a single item by its ID.
    - POST /items/: Creates a new item with the provided data.
    - PUT /items/{item_id}: Updates an existing item with the provided data.
    - DELETE /items/{item_id}: Deletes an item by its ID.

    - FastAPI APIRouter for route management.
    - Item, ItemCreate, ItemUpdate schemas for request and response validation.
    - db: Database access layer for CRUD operations.
"""

import logging
import os
from typing import List

from fastapi import APIRouter, HTTPException

from .schemas import Item

logging.basicConfig(level=logging.ERROR, format="%(asctime)s %(levelname)s %(message)s", filename="app_error.log", filemode="a")


DB_BACKEND = os.getenv("DB_BACKEND", "duckdb")
if DB_BACKEND == "snowflake":
    from .db_snowflake import db as db_manager
else:
    from .db import db as db_manager  # type: ignore

router = APIRouter(prefix="/items", tags=["items"])


@router.get("/", response_model=List[Item])
async def list_items():
    """
    This module defines the API routes for item operations.

    Routes:
        - GET /: Retrieves a list of items from the database.

    Dependencies:
        - FastAPI router
        - Item response model
        - Database access via db.db.fetch_all_items()
    """
    try:
        return await db_manager.fetch_all_items()
    except Exception as e:
        logging.error(f"DB error in list_items: {e}")
        raise HTTPException(status_code=500, detail={"error": str(e)}) from e


@router.get("/{item_id}", response_model=Item)
async def get_item(item_id: int):
    """
    This module defines API routes for item retrieval using FastAPI.

    Routes:
        GET /{item_id}: Retrieve an item by its ID. Returns the item if found, otherwise raises a 404 HTTPException.
    """
    try:
        item = await db_manager.fetch_item(item_id)
        if item is None:
            raise HTTPException(status_code=404, detail="Item not found")
        return item
    except Exception as e:
        logging.error(f"DB error in get_item: {e}")
        raise HTTPException(status_code=500, detail={"error": str(e)}) from e


from fastapi import Request


@router.post("/", response_model=Item, status_code=201)
async def create_item(request: Request):
    """
    This module defines API routes for item management using FastAPI.

    Routes:
        POST /: Creates a new item with the provided name and value.

    Dependencies:
        - db: Database access layer for item creation.
        - Item: Response model representing an item.
        - ItemCreate: Request model for item creation.
    """
    try:
        data = await request.json()
        return await db_manager.create_item(**data)
    except Exception as e:
        logging.error("DB error in create_item: %s", e)
        raise HTTPException(status_code=500, detail={"error": str(e)}) from e


@router.put("/{item_id}", response_model=Item)
async def update_item(item_id: int, request: Request):
    """
    This module defines the API routes for item operations.

    Routes:
        PUT /{item_id}: Updates an existing item with the provided data.
    """
    try:
        data = await request.json()
        updated = await db_manager.update_item(item_id, **data)
        if updated is None:
            raise HTTPException(status_code=404, detail="Item not found")
        return updated
    except Exception as e:
        logging.error("DB error in update_item: %s", e)
        raise HTTPException(status_code=500, detail={"error": str(e)}) from e


@router.delete("/{item_id}", status_code=204)
async def delete_item(item_id: int):
    """
    This module defines API routes for item management.

    Routes:
        DELETE /{item_id}: Deletes an item by its ID. Returns 204 No Content on success,
        or 404 Not Found if the item does not exist.
    """
    try:
        deleted = await db_manager.delete_item(item_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="Item not found")
        return None
    except Exception as e:
        logging.error("DB error in delete_item: %s", e)
        raise HTTPException(status_code=500, detail={"error": str(e)}) from e
