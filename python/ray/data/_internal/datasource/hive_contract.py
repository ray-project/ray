"""Validated inputs for the proposed HiveServer2 read API.

This module defines the API boundary without opening a Hive connection or
creating a Ray read task. The public call shape is specified separately in the
read_hive API contract proposal.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from typing import Literal, Optional, Tuple

import pyarrow as pa

HiveAuthMechanism = Literal["NOSASL", "PLAIN", "GSSAPI"]

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
class HiveConnectionOptions:
    """Connection fields accepted by the initial binary HS2 profile."""

    host: str
    auth_mechanism: HiveAuthMechanism
    port: int = 10000
    user: Optional[str] = None
    password: Optional[str] = field(default=None, repr=False)
    kerberos_service_name: str = "hive"
    use_ssl: bool = False
    ca_cert: Optional[str] = None
    timeout: Optional[float] = None

    def __post_init__(self) -> None:
        if (
            not isinstance(self.host, str)
            or not self.host
            or self.host != self.host.strip()
            or any(char.isspace() or char == "\0" for char in self.host)
        ):
            raise ValueError("host must be a non-empty hostname without whitespace")
        if (
            isinstance(self.port, bool)
            or not isinstance(self.port, int)
            or not 1 <= self.port <= 65535
        ):
            raise ValueError("port must be an integer between 1 and 65535")
        if self.auth_mechanism not in ("NOSASL", "PLAIN", "GSSAPI"):
            raise ValueError("auth_mechanism must be NOSASL, PLAIN, or GSSAPI")
        if self.user is not None and (
            not isinstance(self.user, str)
            or not self.user
            or self.user != self.user.strip()
        ):
            raise ValueError(
                "user must be a non-empty string without surrounding space"
            )
        if self.password is not None and (
            not isinstance(self.password, str) or not self.password
        ):
            raise ValueError("password must be a non-empty string when provided")
        if self.auth_mechanism == "PLAIN":
            if not self.user or not self.password:
                raise ValueError("PLAIN authentication requires user and password")
        elif self.password is not None:
            raise ValueError("password is only supported with PLAIN authentication")
        if (
            not isinstance(self.kerberos_service_name, str)
            or not self.kerberos_service_name
            or self.kerberos_service_name != self.kerberos_service_name.strip()
        ):
            raise ValueError("kerberos_service_name must be a non-empty string")
        if not isinstance(self.use_ssl, bool):
            raise ValueError("use_ssl must be a bool")
        if self.ca_cert is not None and (
            not isinstance(self.ca_cert, str) or not self.ca_cert.strip()
        ):
            raise ValueError("ca_cert must be a non-empty path when provided")
        if self.ca_cert is not None and not self.use_ssl:
            raise ValueError("ca_cert requires use_ssl=True")
        if self.timeout is not None and (
            isinstance(self.timeout, bool)
            or not isinstance(self.timeout, (int, float))
            or not math.isfinite(self.timeout)
            or self.timeout <= 0
        ):
            raise ValueError("timeout must be a positive finite number")


@dataclass(frozen=True)
class HiveReadSpec:
    """A single table or trusted SQL read, before datasource execution."""

    connection: HiveConnectionOptions
    table: Optional[str] = None
    query: Optional[str] = field(default=None, repr=False)
    schema: Optional[pa.Schema] = None
    limit: Optional[int] = None

    def __post_init__(self) -> None:
        if not isinstance(self.connection, HiveConnectionOptions):
            raise TypeError("connection must be HiveConnectionOptions")
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
