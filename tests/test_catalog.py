from pyiceberg_jdbc.catalog import DatabaseCatalog
from sqlalchemy import text
import unittest

from common import create_catalog_tables


class TestDatabaseCatalog(unittest.TestCase):
    def setUp(self):
        self.catalog = DatabaseCatalog("test", uri="sqlite://")
        self.init_fixtures()

    def init_fixtures(self):
        create_catalog_tables(self.catalog.db)
        with self.catalog.db.connect() as conn:
            sql = """
            INSERT INTO iceberg_tables VALUES
            ('test', 'foo', 'bar', '', NULL),
            ('test', 'foo.bar', 'bar', '', NULL),
            ('test', 'a.b', 'bar', '', NULL)
            """
            conn.execute(text(sql))

            sql = """
            INSERT INTO iceberg_namespace_properties VALUES
            ('test', 'foo', 'k1', 'v1'),
            ('test', 'foo', 'k2', 'v2'),
            ('test', 'haha', 'k2', 'v2')
            """
            conn.execute(text(sql))

            conn.commit()

    def test_list_namespaces(self):
        namespaces = self.catalog.list_namespaces()
        self.assertEqual(
            set(namespaces), {("foo",), ("foo", "bar"), ("a", "b"), ("haha",)}
        )

    def test_list_namespaces_with_namespace(self):
        namespaces = self.catalog.list_namespaces("foo")
        self.assertEqual(namespaces, [("foo", "bar")])

    def test_load_namespace_properties(self):
        properties = self.catalog.load_namespace_properties("foo")
        self.assertEqual(properties, {"k1": "v1", "k2": "v2"})
