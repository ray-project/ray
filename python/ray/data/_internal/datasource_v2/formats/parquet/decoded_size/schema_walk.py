"""The walk of a file's Arrow schema into each Parquet leaf's chain of arrays."""

from __future__ import annotations

from typing import Any, NamedTuple

import pyarrow as pa
from pyarrow.parquet import ColumnSchema

from ray.data._internal.datasource_v2.formats.parquet.decoded_size.arrow_layout import (
    _BINARY,
    _FIXED_SIZE_LIST,
    _LIST,
    _STRUCT,
    _is_list,
    _is_repeated,
    _is_zero_copy_type,
    _leaf_layout,
)


class _ArrowNode(NamedTuple):
    """An Arrow array a leaf decodes into: the leaf, or an ancestor it shares."""

    node_id: int
    kind: str
    # No bitmap when False, even under a null parent; but see ``_SchemaWalk.visit``.
    nullable: bool
    # The definition level at which an element of this array is non-null.
    def_level: int
    # Its length: ``scale`` (above 1 only under a fixed_size_list) times the level
    # entries at repetition level ``length_rep_level`` or less and definition
    # level ``length_def_level`` or more.
    length_rep_level: int
    length_def_level: int
    scale: int
    # Bytes per element: a list's or string's offset, a fixed-width leaf's value.
    width: int = 0
    # A bitmap even when null-free: a zero-copy read keeps the one it decoded.
    keeps_bitmap: bool = False


class LeafProfile(NamedTuple):
    """One leaf's schema facts, the same in every row group of the file."""

    # The leaf's Arrow arrays from its top-level column down, the leaf last.
    nodes: tuple[_ArrowNode, ...]
    max_definition_level: int
    max_repetition_level: int
    # The writer counts every DELTA_BYTE_ARRAY value in SizeStatistics bytes.
    counts_delta_byte_array: bool = False


class _Position(NamedTuple):
    """The level context the schema walk hands a child array."""

    # Repeated ancestors so far, which is a leaf's max repetition level.
    depth: int
    # The definition level at which the parent is non-null.
    parent_def: int
    length_rep_level: int
    length_def_level: int
    scale: int


class _SchemaWalk:
    """Flattens an Arrow schema into one node chain per Parquet leaf."""

    def __init__(self) -> None:
        # ``(chain, max_repetition_level)`` per leaf, in schema order.
        self.leaves: list[tuple[tuple[_ArrowNode, ...], int]] = []
        # Each leaf's Arrow type, and the leaf count after each top-level field.
        self.leaf_types: list[pa.DataType] = []
        self.field_ends: list[int] = []
        self._next_id = 0

    def field_leaves(self, field_idx: int) -> range:
        """The leaf indices of top-level field ``field_idx``."""
        start = self.field_ends[field_idx - 1] if field_idx else 0
        return range(start, self.field_ends[field_idx])

    def _node(self, pos: _Position, **fields: Any) -> _ArrowNode:
        """A new array node at the walk's position ``pos``."""
        self._next_id += 1
        return _ArrowNode(
            node_id=self._next_id,
            length_rep_level=pos.length_rep_level,
            length_def_level=pos.length_def_level,
            scale=pos.scale,
            **fields,
        )

    def visit(
        self,
        arrow_type: pa.DataType,
        nullable: bool,
        pos: _Position,
        chain: tuple[_ArrowNode, ...],
    ) -> None:
        """Record the chain of arrays down to each leaf under ``arrow_type``."""
        if isinstance(arrow_type, pa.BaseExtensionType):
            arrow_type = arrow_type.storage_type
        # An optional Parquet node adds one definition level, a required one none.
        def_level = pos.parent_def + int(nullable)
        if pa.types.is_struct(arrow_type):
            self._visit_struct(
                arrow_type=arrow_type,
                nullable=nullable,
                def_level=def_level,
                pos=pos,
                chain=chain,
            )
        elif pa.types.is_map(arrow_type):
            # A map is a list of non-nullable key/value structs.
            self._visit_list(
                arrow_type=arrow_type,
                child_type=pa.struct([arrow_type.key_field, arrow_type.item_field]),
                child_nullable=False,
                nullable=nullable,
                def_level=def_level,
                pos=pos,
                chain=chain,
            )
        elif _is_list(arrow_type):
            self._visit_list(
                arrow_type=arrow_type,
                child_type=arrow_type.value_type,
                child_nullable=arrow_type.value_field.nullable,
                nullable=nullable,
                def_level=def_level,
                pos=pos,
                chain=chain,
            )
        else:
            self._visit_leaf(
                arrow_type=arrow_type,
                nullable=nullable,
                def_level=def_level,
                pos=pos,
                chain=chain,
            )

    def _visit_struct(
        self,
        arrow_type: pa.DataType,
        nullable: bool,
        def_level: int,
        pos: _Position,
        chain: tuple[_ArrowNode, ...],
    ) -> None:
        # PyArrow builds a struct's bitmap from a non-repeated child's levels.
        # With none, even a non-nullable struct gets one, carrying its ancestors'
        # nulls up to the enclosing list.
        hides_nulls = _is_repeated(arrow_type) and def_level > pos.length_def_level
        node = self._node(
            pos=pos,
            kind=_STRUCT,
            nullable=nullable or hides_nulls,
            def_level=def_level,
        )
        for field in arrow_type:
            self.visit(
                arrow_type=field.type,
                nullable=field.nullable,
                pos=pos._replace(parent_def=def_level),
                chain=chain + (node,),
            )

    def _visit_list(
        self,
        arrow_type: pa.DataType,
        child_type: pa.DataType,
        child_nullable: bool,
        nullable: bool,
        def_level: int,
        pos: _Position,
        chain: tuple[_ArrowNode, ...],
    ) -> None:
        # Parquet writes every list type as a repeated group, which adds a
        # definition level (the list is non-empty) and a repetition level.
        rep_def, depth = def_level + 1, pos.depth + 1
        if pa.types.is_fixed_size_list(arrow_type):
            node = self._node(
                pos=pos, kind=_FIXED_SIZE_LIST, nullable=nullable, def_level=def_level
            )
            # Its children are counted at its own depth, ``list_size`` per element.
            inner = pos._replace(
                depth=depth, parent_def=rep_def, scale=pos.scale * arrow_type.list_size
            )
        else:
            node = self._node(
                pos=pos,
                kind=_LIST,
                nullable=nullable,
                def_level=def_level,
                width=8 if pa.types.is_large_list(arrow_type) else 4,
            )
            inner = _Position(
                depth=depth,
                parent_def=rep_def,
                length_rep_level=depth,
                length_def_level=rep_def,
                scale=1,
            )
        self.visit(
            arrow_type=child_type,
            nullable=child_nullable,
            pos=inner,
            chain=chain + (node,),
        )

    def _visit_leaf(
        self,
        arrow_type: pa.DataType,
        nullable: bool,
        def_level: int,
        pos: _Position,
        chain: tuple[_ArrowNode, ...],
    ) -> None:
        kind, width = _leaf_layout(arrow_type)
        leaf = self._node(
            pos=pos,
            kind=kind,
            nullable=nullable,
            def_level=def_level,
            width=width,
            keeps_bitmap=_is_zero_copy_type(arrow_type),
        )
        self.leaves.append((chain + (leaf,), pos.depth))
        self.leaf_types.append(arrow_type)


def _walk(arrow_schema: pa.Schema) -> _SchemaWalk:
    """Walk ``arrow_schema`` into leaf chains."""
    walk = _SchemaWalk()
    top = _Position(
        depth=0, parent_def=0, length_rep_level=0, length_def_level=0, scale=1
    )
    for field in arrow_schema:
        walk.visit(arrow_type=field.type, nullable=field.nullable, pos=top, chain=())
        walk.field_ends.append(len(walk.leaves))
    return walk


def _matches(leaf: _ArrowNode, rep: int, column: ColumnSchema) -> bool:
    """Whether a walked leaf has ``column``'s levels and can decode its values."""
    return (
        leaf.def_level == column.max_definition_level
        and rep == column.max_repetition_level
        and (leaf.kind != _BINARY or column.physical_type == "BYTE_ARRAY")
    )
