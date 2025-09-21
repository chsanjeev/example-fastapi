from fastapi import APIRouter, HTTPException
from typing import List
from . import db
from .schemas import Item, ItemCreate, ItemUpdate

router = APIRouter(prefix="/items", tags=["items"])


@router.get("/", response_model=List[Item])
async def list_items():
    return await db.db.fetch_all_items()


@router.get("/{item_id}", response_model=Item)
async def get_item(item_id: int):
    item = await db.db.fetch_item(item_id)
    if item is None:
        raise HTTPException(status_code=404, detail="Item not found")
    return item


@router.post("/", response_model=Item, status_code=201)
async def create_item(payload: ItemCreate):
    return await db.db.create_item(payload.name, payload.value)


@router.put("/{item_id}", response_model=Item)
async def update_item(item_id: int, payload: ItemUpdate):
    updated = await db.db.update_item(item_id, payload.name, payload.value)
    if updated is None:
        raise HTTPException(status_code=404, detail="Item not found")
    return updated


@router.delete("/{item_id}", status_code=204)
async def delete_item(item_id: int):
    deleted = await db.db.delete_item(item_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Item not found")
    return None
