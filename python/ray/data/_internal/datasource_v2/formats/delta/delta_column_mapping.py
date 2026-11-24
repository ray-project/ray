"""Delta column mapping: the column names a table's data files use.

With column mapping, a Delta table's data files don't name their columns by
the table's (logical) column names. Each column, nested fields included, has a
*physical name* such as ``col-5f4a...`` and a numeric id, both kept in the
field's metadata in the log's schema. Only the log maps them to the logical
names, so renaming a column doesn't rewrite any file.

- In the ``name`` mode, a data file names each column by its physical name.
- In the ``id`` mode, a data file's columns are matched by their Parquet field
  id (``PARQUET:field_id``) to the log's column ids; the names in the file
  don't matter.

``deltalake`` reports the schema with logical names and the physical name and
id in each field's metadata. It reports the ``add`` actions' partition values
under logical names, but their per-file statistics under physical names
(``min.col-5f4a...``) in both modes.

The reader works in logical names throughout and translates only where it
touches the data files: it scans each file by the names that file uses and
renames the result. Statistics are renamed to logical names once, when the
``add`` actions are loaded.
"""

from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
)

import pyarrow as pa

from ray.data.expressions import Expr, col

if TYPE_CHECKING:
    from deltalake import DeltaTable

# Field metadata keys of the log's schema, as ``deltalake`` passes them on.
PHYSICAL_NAME_KEY = b"delta.columnMapping.physicalName"
COLUMN_ID_KEY = b"delta.columnMapping.id"
# Field metadata key of a Parquet field id, as pyarrow reads it.
PARQUET_FIELD_ID_KEY = b"PARQUET:field_id"

# Column-name prefixes of the per-file statistics in
# ``DeltaTable.get_add_actions(flatten=True)``.
_STATISTICS_PREFIXES = ("min.", "max.", "null_count.")


def column_mapping_mode(configuration: Optional[Mapping[str, str]]) -> str:
    """The table's ``delta.columnMapping.mode``, lower-cased; ``"none"`` if unset."""
    return ((configuration or {}).get("delta.columnMapping.mode") or "none").lower()


def _physical_name(field: pa.Field) -> str:
    name = (field.metadata or {}).get(PHYSICAL_NAME_KEY)
    return name.decode() if name is not None else field.name


def _metadata_int(field: pa.Field, key: bytes) -> Optional[int]:
    value = (field.metadata or {}).get(key)
    try:
        return int(value) if value is not None else None
    except ValueError:
        return None


def _renamed_type(
    data_type: pa.DataType, rename: Callable[[pa.Field], str]
) -> pa.DataType:
    """``data_type`` with every nested field named by ``rename``.

    List elements and map keys and values keep their own names, which Delta
    doesn't map.
    """
    if pa.types.is_struct(data_type):
        return pa.struct(
            [
                _renamed_field(data_type.field(i), rename)
                for i in range(data_type.num_fields)
            ]
        )
    if pa.types.is_map(data_type):
        return pa.map_(
            _with_renamed_type(data_type.key_field, rename),
            _with_renamed_type(data_type.item_field, rename),
            keys_sorted=data_type.keys_sorted,
        )
    if pa.types.is_fixed_size_list(data_type):
        return pa.list_(
            _with_renamed_type(data_type.value_field, rename), data_type.list_size
        )
    if pa.types.is_large_list(data_type):
        return pa.large_list(_with_renamed_type(data_type.value_field, rename))
    if pa.types.is_list(data_type):
        return pa.list_(_with_renamed_type(data_type.value_field, rename))
    return data_type


def _with_renamed_type(field: pa.Field, rename: Callable[[pa.Field], str]) -> pa.Field:
    return field.with_type(_renamed_type(field.type, rename))


def _renamed_field(field: pa.Field, rename: Callable[[pa.Field], str]) -> pa.Field:
    return pa.field(
        rename(field),
        _renamed_type(field.type, rename),
        nullable=field.nullable,
        metadata=field.metadata,
    )


def _fields_by_parquet_id(fields: Iterable[pa.Field]) -> Dict[int, pa.Field]:
    by_id = {}
    for field in fields:
        field_id = _metadata_int(field, PARQUET_FIELD_ID_KEY)
        if field_id is not None:
            by_id[field_id] = field
    return by_id


def _id_matched_field(
    table_field: pa.Field, file_fields_by_id: Mapping[int, pa.Field]
) -> pa.Field:
    """``table_field`` named as the file names the field with its column id.

    A field the file doesn't have keeps its physical name, which the file
    doesn't use either, so the scan fills it with nulls.
    """
    column_id = _metadata_int(table_field, COLUMN_ID_KEY)
    file_field = None if column_id is None else file_fields_by_id.get(column_id)
    return pa.field(
        file_field.name if file_field is not None else _physical_name(table_field),
        _id_matched_type(
            table_field.type, file_field.type if file_field is not None else None
        ),
        nullable=table_field.nullable,
        metadata=table_field.metadata,
    )


def _id_matched_type(
    table_type: pa.DataType, file_type: Optional[pa.DataType]
) -> pa.DataType:
    if pa.types.is_struct(table_type):
        file_children = (
            _fields_by_parquet_id(file_type)
            if file_type is not None and pa.types.is_struct(file_type)
            else {}
        )
        return pa.struct(
            [
                _id_matched_field(table_type.field(i), file_children)
                for i in range(table_type.num_fields)
            ]
        )
    if pa.types.is_map(table_type):
        file_map = (
            file_type if file_type is not None and pa.types.is_map(file_type) else None
        )
        return pa.map_(
            table_type.key_field.with_type(
                _id_matched_type(
                    table_type.key_type, file_map.key_type if file_map else None
                )
            ),
            table_type.item_field.with_type(
                _id_matched_type(
                    table_type.item_type, file_map.item_type if file_map else None
                )
            ),
            keys_sorted=table_type.keys_sorted,
        )
    if (
        pa.types.is_list(table_type)
        or pa.types.is_large_list(table_type)
        or pa.types.is_fixed_size_list(table_type)
    ):
        file_value_type = (
            file_type.value_type
            if file_type is not None
            and (
                pa.types.is_list(file_type)
                or pa.types.is_large_list(file_type)
                or pa.types.is_fixed_size_list(file_type)
            )
            else None
        )
        value_field = table_type.value_field.with_type(
            _id_matched_type(table_type.value_type, file_value_type)
        )
        if pa.types.is_fixed_size_list(table_type):
            return pa.list_(value_field, table_type.list_size)
        if pa.types.is_large_list(table_type):
            return pa.large_list(value_field)
        return pa.list_(value_field)
    return table_type


def _view(column: pa.ChunkedArray, data_type: pa.DataType) -> pa.ChunkedArray:
    return pa.chunked_array(
        [chunk.view(data_type) for chunk in column.chunks], type=data_type
    )


def _statistics_paths(field: pa.Field, physical: str, logical: str) -> Iterable[tuple]:
    """``(physical path, logical path)`` of ``field`` and its nested struct fields."""
    yield physical, logical
    if pa.types.is_struct(field.type):
        for i in range(field.type.num_fields):
            child = field.type.field(i)
            yield from _statistics_paths(
                child, f"{physical}.{_physical_name(child)}", f"{logical}.{child.name}"
            )


@dataclass(frozen=True)
class DeltaColumnMapping:
    """How a column-mapped Delta table names its columns in its data files.

    The methods that name columns as a data file does take that file's own
    schema (``file_schema``), which the ``id`` mode needs to match columns by
    their Parquet field ids. In the ``name`` mode, every file uses the
    physical names, so ``file_schema`` isn't needed.

    Attributes:
        mode: The table's ``delta.columnMapping.mode``: ``"name"`` or ``"id"``.
        schema: The table schema, with logical names. Each field's metadata,
            nested fields' included, holds its physical name and column id.
    """

    mode: str
    schema: pa.Schema

    @classmethod
    def of(
        cls, configuration: Optional[Mapping[str, str]], schema: pa.Schema
    ) -> Optional["DeltaColumnMapping"]:
        """The table's column mapping, or ``None`` if it maps no columns.

        Args:
            configuration: The table's ``metadata().configuration``.
            schema: The table schema, as ``DeltaTable.schema().to_arrow()``
                reports it.

        Returns:
            The column mapping, or ``None`` for the mode ``none``.
        """
        mode = column_mapping_mode(configuration)
        return None if mode == "none" else cls(mode=mode, schema=schema)

    @classmethod
    def of_table(cls, table: "DeltaTable") -> Optional["DeltaColumnMapping"]:
        """``table``'s column mapping, or ``None`` if it maps no columns."""
        return cls.of(
            table.metadata().configuration, pa.schema(table.schema().to_arrow())
        )

    def _file_field(
        self, table_field: pa.Field, file_schema: Optional[pa.Schema]
    ) -> pa.Field:
        """``table_field`` named as a data file names it, nested fields too."""
        if self.mode == "id" and file_schema is not None:
            return _id_matched_field(table_field, _fields_by_parquet_id(file_schema))
        return _renamed_field(table_field, _physical_name)

    def _table_field(self, logical: str) -> Optional[pa.Field]:
        index = self.schema.get_field_index(logical)
        return None if index == -1 else self.schema.field(index)

    def physical_name(
        self, logical: str, file_schema: Optional[pa.Schema] = None
    ) -> str:
        """The data file's name for the table column ``logical``.

        A name that isn't a table column, such as a synthesized one, is
        returned unchanged.
        """
        table_field = self._table_field(logical)
        if table_field is None:
            return logical
        return self._file_field(table_field, file_schema).name

    def physical_names(
        self, logical: Iterable[str], file_schema: Optional[pa.Schema] = None
    ) -> List[str]:
        return [self.physical_name(name, file_schema) for name in logical]

    def physical_schema(
        self, logical: pa.Schema, file_schema: Optional[pa.Schema] = None
    ) -> pa.Schema:
        """``logical``, a subset of the table schema, as the data file names it.

        Fields that aren't table columns keep their names.
        """
        fields = []
        for field in logical:
            # The table's own field carries the physical names and ids, nested
            # ones too.
            table_field = self._table_field(field.name)
            if table_field is None:
                fields.append(field)
                continue
            file_field = self._file_field(table_field, file_schema)
            fields.append(
                pa.field(
                    file_field.name,
                    file_field.type,
                    nullable=field.nullable,
                    metadata=field.metadata,
                )
            )
        return pa.schema(fields)

    def physical_predicate(
        self, predicate: Optional[Expr], file_schema: Optional[pa.Schema] = None
    ) -> Optional[Expr]:
        """``predicate`` with its column references renamed as the file names them."""
        if predicate is None:
            return None
        from ray.data._internal.planner.plan_expression.expression_visitors import (
            _ColumnSubstitutionVisitor,
        )

        renames: Dict[str, Expr] = {
            field.name: col(self._file_field(field, file_schema).name)
            for field in self.schema
        }
        return _ColumnSubstitutionVisitor(renames).visit(predicate)

    def is_nested(self, logical: str) -> bool:
        """Whether the table column ``logical`` has nested fields to map."""
        table_field = self._table_field(logical)
        return table_field is not None and pa.types.is_nested(table_field.type)

    def to_logical(
        self, table: pa.Table, file_schema: Optional[pa.Schema] = None
    ) -> pa.Table:
        """Rename ``table``'s columns, read with the data file's names, and their
        nested fields, to the table's logical names.

        A column of exactly the file-named type is relabelled without copying
        (``Array.view``). One whose type differs otherwise, which only a scan
        without the table schema pinned produces, keeps its type and only
        takes the logical name.
        """
        by_file_name = {}
        for field in self.schema:
            file_field = self._file_field(field, file_schema)
            by_file_name[file_field.name] = (field, file_field.type)
        names = []
        columns = []
        for name, column in zip(table.column_names, table.columns):
            match = by_file_name.get(name)
            if match is not None:
                field, file_type = match
                name = field.name
                if column.type == file_type:
                    column = _view(column, field.type)
            names.append(name)
            columns.append(column)
        return pa.Table.from_arrays(columns, names=names)

    def statistics_renames(self, column_names: Iterable[str]) -> Dict[str, str]:
        """Renames of the ``add`` actions' statistics columns to logical names.

        ``get_add_actions(flatten=True)`` keys statistics by physical name,
        nested fields as dotted paths (``min.col-1.col-2``), in both modes.

        Args:
            column_names: The ``add`` actions' column names.

        Returns:
            The statistics columns to rename, each to its logical name.
        """
        paths = {}
        for field in self.schema:
            paths.update(_statistics_paths(field, _physical_name(field), field.name))
        renames = {}
        for name in column_names:
            for prefix in _STATISTICS_PREFIXES:
                if name.startswith(prefix) and name[len(prefix) :] in paths:
                    renames[name] = prefix + paths[name[len(prefix) :]]
        return renames
