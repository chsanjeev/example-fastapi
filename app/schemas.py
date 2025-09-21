from pydantic import BaseModel
from typing import Optional


class ItemCreate(BaseModel):
    name: str
    value: Optional[str] = None


class Item(ItemCreate):
    id: int


class ItemUpdate(BaseModel):
    name: str
    value: Optional[str] = None
