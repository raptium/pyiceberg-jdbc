from sqlalchemy import text


def create_catalog_tables(db):
    with db.connect() as conn:
        sql = """
        CREATE TABLE iceberg_tables (
            catalog_name VARCHAR(255) NOT NULL,
            table_namespace VARCHAR(255) NOT NULL,
            table_name VARCHAR(255) NOT NULL,
            metadata_location VARCHAR(255) NOT NULL,
            previous_metadata_location VARCHAR(255)
        )
        """
        conn.execute(text(sql))
        sql = """
        CREATE TABLE iceberg_namespace_properties (
            catalog_name VARCHAR(255) NOT NULL,
            namespace VARCHAR(255) NOT NULL,
            property_key VARCHAR(255) NOT NULL,
            property_value VARCHAR(255) NOT NULL
        )
        """
        conn.execute(text(sql))
