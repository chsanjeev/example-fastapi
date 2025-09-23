# Table-specific configuration for API routing
# You can extend this dictionary for more tables and custom logic

table_config = {
    "items": {"order_by": "id", "where": ""},
    "products": {"order_by": "product_id", "where": "is_active = 1"},
    # Add more tables as needed
}
