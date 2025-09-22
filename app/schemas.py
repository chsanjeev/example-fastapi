"""
This module defines schema types for the FastAPI application.

Attributes:
    Item (Dict[str, Any]): A dynamic response model representing an item as a dictionary
        with string keys and values of any type.
"""

from typing import Any, Dict

# Dynamic response model for items
Item = Dict[str, Any]
