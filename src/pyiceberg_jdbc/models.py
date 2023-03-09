from sqlalchemy import Table, Column, String, MetaData

metadata = MetaData()

IcebergTables = Table(
    "iceberg_tables",
    metadata,
    Column("catalog_name", String),
    Column("table_namespace", String),
    Column("table_name", String),
    Column("metadata_location", String),
    Column("previous_metadata_location", String),
)

IcebergNamespaceProperties = Table(
    "iceberg_namespace_properties",
    metadata,
    Column("catalog_name", String),
    Column("namespace", String),
    Column("property_key", String),
    Column("property_value", String),
)
