"""Delta column mapping: the column names a table's data files use.

With column mapping, a Delta table's data files don't name their columns by
the table's (logical) column names. Each column, nested fields included, has a
*physical name* such as ``col-5f4a...`` and a numeric id, both kept in the
field's metadata in the log's schema. Only the log maps them to the logical
names, so renaming a column doesn't rewrite any file.

``deltalake`` reports the schema with logical names and the physical name in
each field's metadata (``delta.columnMapping.physicalName``). It reports the
``add`` actions' partition values under logical names, but their per-file
statistics under physical names (``min.col-5f4a...``).

The reader works in logical names throughout and translates only where it
touches the data files: it scans each file by physical name and renames the
result. Statistics are renamed to logical names once, when the ``add`` actions
are loaded.
"""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, Dict, Iterable, List, Mapping, Optional

import pyarrow as pa

from ray.data.expressions import Expr, col

if TYPE_CHECKING:
    from deltalake import DeltaTable

# Field metadata keys of the log's schema, as ``deltalake`` passes them on.
PHYSICAL_NAME_KEY = b"delta.columnMapping.physicalName"

# Column-name prefixes of the per-file statistics in
# ``DeltaTable.get_add_actions(flatten=True)``.
_STATISTICS_PREFIXES = ("min.", "max.", "null_count.")


def column_mapping_mode(configuration: Optional[Mapping[str, str]]) -> str:
    """The table's ``delta.columnMapping.mode``, lower-cased; ``"none"`` if unset."""
    return ((configuration or {}).get("delta.columnMapping.mode") or "none").lower()


def _physical_name(field: pa.Field) -> str:
    name = (field.metadata or {}).get(PHYSICAL_NAME_KEY)
    return name.decode() if name is not None else field.name


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

    Attributes:
        mode: The table's ``delta.columnMapping.mode``: ``"name"``.
        schema: The table schema, with logical names. Each field's metadata,
            nested fields' included, holds its physical name.
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

    def physical_name(self, logical: str) -> str:
        """The data files' name for the table column ``logical``.

        A name that isn't a table column, such as a synthesized one, is
        returned unchanged.
        """
        index = self.schema.get_field_index(logical)
        return logical if index == -1 else _physical_name(self.schema.field(index))

    def physical_names(self, logical: Iterable[str]) -> List[str]:
        return [self.physical_name(name) for name in logical]

    def physical_schema(self, logical: pa.Schema) -> pa.Schema:
        """``logical``, a subset of the table schema, as the data files name it.

        Fields that aren't table columns keep their names.
        """
        by_name = {field.name: field for field in self.schema}
        fields = []
        for field in logical:
            # The table's own field carries the physical names, nested ones too.
            table_field = by_name.get(field.name)
            fields.append(
                field
                if table_field is None
                else pa.field(
                    _physical_name(table_field),
                    _renamed_type(table_field.type, _physical_name),
                    nullable=field.nullable,
                    metadata=field.metadata,
                )
            )
        return pa.schema(fields)

    def physical_predicate(self, predicate: Optional[Expr]) -> Optional[Expr]:
        """``predicate`` with its column references renamed to physical names."""
        if predicate is None:
            return None
        from ray.data._internal.planner.plan_expression.expression_visitors import (
            _ColumnSubstitutionVisitor,
        )

        renames: Dict[str, Expr] = {
            field.name: col(_physical_name(field)) for field in self.schema
        }
        return _ColumnSubstitutionVisitor(renames).visit(predicate)

    def is_nested(self, logical: str) -> bool:
        """Whether the table column ``logical`` has nested fields to map."""
        index = self.schema.get_field_index(logical)
        if index == -1:
            return False
        data_type = self.schema.field(index).type
        return pa.types.is_nested(data_type)

    def to_logical(self, table: pa.Table) -> pa.Table:
        """Rename ``table``'s physical columns, and their nested fields, to the
        table's logical names.

        A column of exactly the physical type is relabelled without copying
        (``Array.view``). One whose type differs otherwise, which only a scan
        without the table schema pinned produces, keeps its type and only
        takes the logical name.
        """
        by_physical = {_physical_name(field): field for field in self.schema}
        names = []
        columns = []
        for name, column in zip(table.column_names, table.columns):
            field = by_physical.get(name)
            if field is not None:
                name = field.name
                if column.type == _renamed_type(field.type, _physical_name):
                    column = _view(column, field.type)
            names.append(name)
            columns.append(column)
        return pa.Table.from_arrays(columns, names=names)

    def statistics_renames(self, column_names: Iterable[str]) -> Dict[str, str]:
        """Renames of the ``add`` actions' statistics columns to logical names.

        ``get_add_actions(flatten=True)`` keys statistics by physical name,
        nested fields as dotted paths (``min.col-1.col-2``).

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
