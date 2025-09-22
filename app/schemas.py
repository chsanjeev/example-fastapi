from typing import Optional

from pydantic import BaseModel


class ItemCreate(BaseModel):
    """
    Schema for creating an Item.

    Attributes:
        name (str): The name of the item.
        value (Optional[str]): An optional value associated with the item.
    """

    name: str
    value: Optional[str] = None


class Item(ItemCreate):
    """
    Represents an item with a unique identifier.

    Inherits from:
        ItemCreate: Base schema for item creation.

    Attributes:
        id (int): Unique identifier for the item.
    """

    id: int


class ItemUpdate(BaseModel):
    """
    Schema for updating an item.

    Attributes:
        name (str): The name of the item.
        value (Optional[str]): The value associated with the item. Defaults to None.
    """

    name: str
    value: Optional[str] = None
