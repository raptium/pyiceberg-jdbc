from __future__ import annotations

from typing import Union, List, Optional

from pyiceberg.catalog import Catalog, PropertiesUpdateSummary, URI
from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError
from pyiceberg.io import load_file_io, FileIO
from pyiceberg.partitioning import PartitionSpec, UNPARTITIONED_PARTITION_SPEC
from pyiceberg.schema import Schema
from pyiceberg.serializers import FromInputFile
from pyiceberg.table import Table, SortOrder
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER
from pyiceberg.typedef import Identifier, Properties, EMPTY_DICT
from sqlalchemy import create_engine, select

from pyiceberg_jdbc.models import IcebergTables, IcebergNamespaceProperties


class DatabaseCatalog(Catalog):
    def __init__(self, name: str, **properties: str):
        self.name = name
        self.uri = properties[URI]
        self._init_db()
        super().__init__(name, **properties)

    def _init_db(self):
        self.db = create_engine(self.uri)

    def _fetch_configs(self, properties: dict[str, str]) -> dict[str, str]:
        props = dict(properties)
        props.pop(URI)
        return props

    def _load_file_io(self, properties: Properties = EMPTY_DICT) -> FileIO:
        return load_file_io({**self.properties, **properties})

    def create_table(
        self,
        identifier: Union[str, Identifier],
        schema: Schema,
        location: Optional[str] = None,
        partition_spec: PartitionSpec = UNPARTITIONED_PARTITION_SPEC,
        sort_order: SortOrder = UNSORTED_SORT_ORDER,
        properties: Properties = EMPTY_DICT,
    ) -> Table:
        raise NotImplemented

    def load_table(self, identifier: Union[str, Identifier]) -> Table:
        namespace = self.namespace_from(identifier)
        if not self._namespace_exists(namespace):
            raise NoSuchNamespaceError()
        table_name = self.table_name_from(identifier)
        with self.db.connect() as conn:
            query = (
                select(IcebergTables.c.metadata_location)
                .where(IcebergTables.c.catalog_name == self.name)
                .where(
                    IcebergTables.c.table_namespace == self.identifier_to_str(namespace)
                )
                .where(IcebergTables.c.table_name == table_name)
                .limit(1)
            )
            result = conn.execute(query)
            if result.rowcount == 0:
                raise NoSuchTableError()
            metadata_location = result.fetchone()[0]
            return self._load_table(identifier, metadata_location)

    def drop_table(self, identifier: Union[str, Identifier]) -> None:
        raise NotImplemented

    def purge_table(self, identifier: Union[str, Identifier]) -> None:
        raise NotImplemented

    def rename_table(
        self,
        from_identifier: Union[str, Identifier],
        to_identifier: Union[str, Identifier],
    ) -> Table:
        raise NotImplemented

    def create_namespace(
        self, namespace: Union[str, Identifier], properties: Properties = EMPTY_DICT
    ) -> None:
        raise NotImplemented

    def drop_namespace(self, namespace: Union[str, Identifier]) -> None:
        raise NotImplemented

    def list_tables(self, namespace: Union[str, Identifier]) -> List[Identifier]:
        if not self._namespace_exists(namespace):
            raise NoSuchNamespaceError()
        namespace = self.identifier_to_tuple(namespace)
        with self.db.connect() as conn:
            query = (
                select(IcebergTables.c.table_name)
                .where(IcebergTables.c.catalog_name == self.name)
                .where(
                    IcebergTables.c.table_namespace == self.identifier_to_str(namespace)
                )
            )
            return [namespace + (c[0],) for c in conn.execute(query)]

    def list_namespaces(
        self, namespace: Union[str, Identifier] = ()
    ) -> List[Identifier]:
        namespaces = set([])
        level = len(self.identifier_to_tuple(namespace))
        if level > 0:
            if not self._namespace_exists(namespace):
                raise NoSuchNamespaceError()
        with self.db.connect() as conn:
            filters = [IcebergTables.c.catalog_name == self.name]
            if level > 0:
                filters.append(
                    IcebergTables.c.table_namespace.like(
                        f"{self.identifier_to_str(namespace)}%"
                    )
                )
            query = select(IcebergTables.c.table_namespace).distinct().where(*filters)
            namespaces.update(
                [self.identifier_to_tuple(c[0]) for c in conn.execute(query)]
            )

            filters = [IcebergNamespaceProperties.c.catalog_name == self.name]
            if level > 0:
                filters.append(
                    IcebergNamespaceProperties.c.namespace.like(
                        f"{self.identifier_to_str(namespace)}%"
                    )
                )
            query = (
                select(IcebergNamespaceProperties.c.namespace)
                .distinct()
                .where(*filters)
            )
            namespaces.update(
                [self.identifier_to_tuple(c[0]) for c in conn.execute(query)]
            )

        if level > 0:
            # exclude self, only include next level
            namespaces = [ns for ns in namespaces if len(ns) == level + 1]

        return list(namespaces)

    def load_namespace_properties(
        self, namespace: Union[str, Identifier]
    ) -> Properties:
        if not self._namespace_exists(namespace):
            raise NoSuchNamespaceError()
        with self.db.connect() as conn:
            query = select(
                IcebergNamespaceProperties.c.property_key,
                IcebergNamespaceProperties.c.property_value,
            ).where(IcebergNamespaceProperties.c.namespace == namespace)
            return {c[0]: c[1] for c in conn.execute(query)}

    def update_namespace_properties(
        self,
        namespace: Union[str, Identifier],
        removals: set[str] | None = None,
        updates: Properties = EMPTY_DICT,
    ) -> PropertiesUpdateSummary:
        raise NotImplemented

    def _namespace_exists(self, namespace: Union[str, Identifier]) -> bool:
        namespace = self.identifier_to_str(namespace)
        with self.db.connect() as conn:
            query = (
                select(IcebergTables.c.table_name)
                .where(IcebergTables.c.table_namespace == namespace)
                .limit(1)
            )
            if conn.execute(query).first() is not None:
                return True
            query = (
                select(IcebergNamespaceProperties.c.property_key)
                .where(IcebergNamespaceProperties.c.namespace == namespace)
                .limit(1)
            )
            if conn.execute(query).first() is not None:
                return True
        return False

    def _load_table(self, identifier: Identifier, metadata_location: str) -> Table:
        io = load_file_io(properties=self.properties, location=metadata_location)
        file = io.new_input(metadata_location)
        metadata = FromInputFile.table_metadata(file)
        return Table(
            identifier=identifier,
            metadata=metadata,
            metadata_location=metadata_location,
            io=self._load_file_io(metadata.properties),
        )

    @staticmethod
    def identifier_to_str(identifier: Union[str, Identifier]) -> str:
        return identifier if isinstance(identifier, str) else ".".join(identifier)
