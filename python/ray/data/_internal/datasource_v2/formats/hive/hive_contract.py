"""Validated inputs for the HiveServer2 read API.

This module defines the API boundary without opening a Hive connection or
creating a Ray read task. The public read_hive facade is defined in
ray.data.read_api.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, Tuple

import pyarrow as pa

_IDENTIFIER_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


def _parse_table_identifier(table: str) -> Tuple[str, str]:
    """Parse the initial table-name subset without accepting SQL fragments."""
    if not isinstance(table, str):
        raise ValueError("table must be a string")

    parts = table.split(".")
    if len(parts) == 1:
        parts.insert(0, "default")
    if len(parts) != 2 or any(not _IDENTIFIER_RE.fullmatch(part) for part in parts):
        raise ValueError(
            "table must be a simple Hive identifier, optionally qualified by database"
        )
    return parts[0], parts[1]


@dataclass(frozen=True)
class HiveReadSpec:
    """A single table or trusted SQL read, before datasource execution."""

    connection_factory: Callable[[], Any] = field(repr=False)
    user: Optional[str] = None
    table: Optional[str] = None
    query: Optional[str] = field(default=None, repr=False)
    schema: Optional[pa.Schema] = None
    limit: Optional[int] = None

    def __post_init__(self) -> None:
        if not callable(self.connection_factory):
            raise TypeError("connection_factory must be callable")
        if self.user is not None and (
            not isinstance(self.user, str)
            or not self.user
            or self.user != self.user.strip()
        ):
            raise ValueError(
                "user must be a non-empty string without surrounding space"
            )
        if (self.table is None) == (self.query is None):
            raise ValueError("specify exactly one of table or query")
        if self.limit is not None and (
            isinstance(self.limit, bool)
            or not isinstance(self.limit, int)
            or self.limit < 0
        ):
            raise ValueError("limit must be a non-negative integer")

        if self.table is not None:
            _parse_table_identifier(self.table)
            if self.schema is not None:
                raise ValueError("schema is only supported for query reads")
        else:
            if (
                not isinstance(self.query, str)
                or not self.query.strip()
                or "\0" in self.query
            ):
                raise ValueError("query must be a non-empty SQL string")
            if not isinstance(self.schema, pa.Schema) or len(self.schema) == 0:
                raise ValueError("query reads require a non-empty pyarrow.Schema")
            if len({name.casefold() for name in self.schema.names}) != len(
                self.schema.names
            ):
                raise ValueError("query schema must have unique column names")
            if self.limit is not None:
                raise ValueError("limit is only supported for table reads")

    @property
    def table_identifier(self) -> Optional[Tuple[str, str]]:
        """Return the database and table name for table reads."""
        return _parse_table_identifier(self.table) if self.table is not None else None
