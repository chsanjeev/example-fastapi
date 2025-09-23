from typing import Any

from pydantic import create_model


def infer_type(value: Any):
    """
    derive datatypes during the code execution.
    """
    if isinstance(value, bool):
        return (bool, ...)
    elif isinstance(value, int):
        return (int, ...)
    elif isinstance(value, float):
        return (float, ...)
    elif isinstance(value, list):
        return (list[Any], ...)
    elif isinstance(value, dict):
        nested_fields = {k: infer_type(v) for k, v in value.items()}
        nested_model = create_model("NestedModel", **nested_fields)
        return (nested_model, ...)
    else:
        return (str, ...)


def python_to_duckdb_type(value: Any) -> str:
    if isinstance(value, bool):
        return "BOOLEAN"
    elif isinstance(value, int):
        return "INTEGER"
    elif isinstance(value, float):
        return "DOUBLE"
    elif isinstance(value, list) or isinstance(value, dict):
        return "JSON"
    else:
        return "TEXT"
