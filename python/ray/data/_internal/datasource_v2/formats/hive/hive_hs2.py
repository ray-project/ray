"""HiveServer2 connection, schema, and row conversion helpers."""

import re
import ssl
from contextlib import suppress
from typing import Any, Callable, Iterator, List, Optional, Tuple

import pyarrow as pa

from ray.data._internal.datasource_v2.formats.hive.hive_contract import (
    HiveReadSpec,
)

_FETCH_ROWS = 1024
_DECIMAL = re.compile(r"decimal\((\d+),(\d+)\)\Z", re.IGNORECASE)
_CHAR = re.compile(r"(?:var)?char\(\d+\)\Z", re.IGNORECASE)
_HIVE_TYPES = {
    "boolean": pa.bool_(),
    "tinyint": pa.int8(),
    "smallint": pa.int16(),
    "int": pa.int32(),
    "integer": pa.int32(),
    "bigint": pa.int64(),
    "float": pa.float32(),
    "double": pa.float64(),
    "string": pa.string(),
    "char": pa.string(),
    "varchar": pa.string(),
    "binary": pa.binary(),
    "date": pa.date32(),
    # Impyla truncates sub-microsecond digits when decoding TIMESTAMP to datetime.
    # Keep the values timezone-naive; this reader does not infer or convert time zones.
    "timestamp": pa.timestamp("us"),
}


def _arrow_type(hive_type: str) -> pa.DataType:
    normalized = hive_type.strip().lower().replace(" ", "")
    if normalized in _HIVE_TYPES:
        return _HIVE_TYPES[normalized]
    if _CHAR.fullmatch(normalized):
        return pa.string()
    decimal = _DECIMAL.fullmatch(normalized)
    if decimal:
        precision, scale = (int(part) for part in decimal.groups())
        if 1 <= precision <= 38 and 0 <= scale <= precision:
            return pa.decimal128(precision, scale)
    raise ValueError("HiveServer2 table has an unsupported column type")


def _iter_exception_chain(exc: BaseException) -> Iterator[BaseException]:
    seen = set()
    curr: Optional[BaseException] = exc
    while curr is not None and id(curr) not in seen:
        seen.add(id(curr))
        yield curr
        curr = curr.__cause__ or curr.__context__


def _classify_error_detail(exc: BaseException) -> Optional[str]:
    """Inspect the exception and its causes for TLS, Kerberos, or credential issues."""
    for e in _iter_exception_chain(exc):
        msg = str(e).lower()
        err_type = type(e).__name__

        # 1. TLS Hostname Mismatch
        has_hostname_kw = any(
            kw in msg
            for kw in (
                "hostname mismatch",
                "doesn't match",
                "does not match",
                "ip address mismatch",
                "certificate is not valid for",
            )
        )
        if has_hostname_kw and any(
            t in msg for t in ("certificate", "cert", "ssl", "tls", "hostname", "san")
        ):
            return f"TLS certificate verification failed due to hostname mismatch ({e})"

        # 2. TLS Certificate Verification Failure / Untrusted Certificate
        is_cert_verify_error = (
            isinstance(e, ssl.SSLCertVerificationError)
            or err_type == "SSLCertVerificationError"
        )
        has_cert_verify_kw = any(
            kw in msg
            for kw in (
                "certificate_verify_failed",
                "certificate verify failed",
                "self-signed certificate",
                "unable to get local issuer certificate",
                "certificate has expired",
                "untrusted certificate",
                "unable to verify the first certificate",
                "certificate verification failed",
            )
        )
        if is_cert_verify_error or has_cert_verify_kw:
            if "expired" in msg:
                return f"TLS certificate verification failed (certificate has expired: {e})"
            return (
                "TLS certificate verification failed "
                f"(untrusted certificate or self-signed certificate in chain: {e})"
            )

        # 3. TLS Certificate or Credential File Not Found or Invalid
        is_file_error = isinstance(e, FileNotFoundError) or (
            isinstance(e, OSError)
            and not isinstance(e, ssl.SSLError)
            and any(
                kw in msg
                for kw in (
                    "ca_cert",
                    "ca_certs",
                    "certificate",
                    "cert",
                    "pem",
                    "keyfile",
                    "keytab",
                )
            )
        )
        has_pem_or_cert_file_kw = isinstance(e, ssl.SSLError) and (
            "no such file or directory" in msg or "cannot load" in msg or "pem" in msg
        )
        if is_file_error or has_pem_or_cert_file_kw:
            return f"TLS certificate or credential file error ({e})"

        # 4. General TLS/SSL Handshake Failure
        if isinstance(e, ssl.SSLError) or any(
            kw in msg
            for kw in (
                "wrong_version_number",
                "unknown_protocol",
                "sslv3_alert",
                "tlsv1_alert",
                "handshake_failure",
            )
        ):
            return f"TLS/SSL handshake failed ({e})"

        # 5. Kerberos/GSSAPI or SASL Missing Dependencies
        if isinstance(e, (ImportError, ModuleNotFoundError)) and any(
            kw in msg for kw in ("kerberos", "gssapi", "thrift_sasl")
        ):
            if "thrift_sasl" in msg:
                return (
                    f"SASL dependency missing ({e}); "
                    "install thrift_sasl and ensure required libraries are available"
                )
            return (
                f"Kerberos/GSSAPI dependency missing ({e}); "
                "install impyla[kerberos] and ensure kerberos/gssapi libraries are available"
            )

        # 6. Kerberos/GSSAPI Service Configuration Error
        if any(
            kw in msg
            for kw in (
                "server not found in kerberos database",
                "hostname cannot be canonicalized",
                "wrong principal",
            )
        ):
            return (
                f"Kerberos/GSSAPI service configuration error "
                f"(check kerberos_service_name and server hostname: {e})"
            )

        # 7. Kerberos/GSSAPI Credentials Missing or Expired
        is_gss_error = err_type in ("GSSError", "KerberosException")
        has_kerberos_cred_kw = any(
            kw in msg
            for kw in (
                "no kerberos credentials available",
                "ticket expired",
                "credentials cache",
                "cannot find kdc for realm",
                "major (851968)",
                "gssapi error",
                "ccache",
            )
        ) or (
            "sasl_client_start" in msg
            and any(kw in msg for kw in ("gssapi", "kerberos"))
        )
        if is_gss_error or has_kerberos_cred_kw:
            return (
                f"Kerberos/GSSAPI authentication failed "
                f"(credentials missing, ticket expired, or credentials cache invalid: {e})"
            )

        # 8. Authorization / Permission Denied
        if any(
            kw in msg
            for kw in (
                "permission denied",
                "not authorized",
                "authorization failed",
            )
        ):
            return f"Authorization failed (permission denied or insufficient privileges: {e})"

        # 9. Authentication / Credential Failure (PLAIN, LDAP, SASL)
        if any(
            kw in msg
            for kw in (
                "authentication failed",
                "invalid credentials",
                "bad credentials",
                "sasl(-13)",
                "user not found",
                "plain auth failed",
                "plain authentication requires a password",
                "requires hive_test_password",
                "missing password",
                "missing credentials",
                "bad status: 3",
            )
        ):
            return f"Authentication failed (missing or invalid credentials: {e})"

    return None


def _connect(connection_factory: Callable[[], Any]):
    try:
        connection = connection_factory()
    except Exception as exc:
        detail = _classify_error_detail(exc)
        if detail:
            raise RuntimeError(f"HiveServer2 connection failed: {detail}") from exc
        raise RuntimeError("HiveServer2 connection failed") from exc

    if connection is None:
        raise TypeError(
            "HiveServer2 connection_factory returned None; expected a connection object"
        )
    
    has_cursor = hasattr(connection, "cursor") and callable(
        getattr(connection, "cursor", None)
    )
    has_close = hasattr(connection, "close") and callable(
        getattr(connection, "close", None)
    )

    if not has_cursor or not has_close:
        if has_close:
            with suppress(Exception):
                connection.close()
        missing_method = "cursor" if not has_cursor else "close"
        raise TypeError(
            "HiveServer2 connection_factory returned an invalid connection object "
            f"(missing callable '{missing_method}' method)"
        )
    return connection


def _metadata_pattern(identifier: str) -> str:
    """Escape HS2 GetColumns wildcards so the identifier matches only itself.

    HiveServer2 treats ``_`` and ``%`` in GetColumns schema/table arguments as
    single- and multi-character wildcards, and ``\\`` as the escape character.
    """
    return identifier.replace("\\", "\\\\").replace("_", "\\_").replace("%", "\\%")


def infer_table_schema(spec: HiveReadSpec) -> pa.Schema:
    """Use the HS2 metadata operation; never execute a data query."""
    table_identifier = spec.table_identifier
    if table_identifier is None:
        raise ValueError("table schema inference requires a table read")
    database, table = table_identifier
    connection = _connect(spec.connection_factory)
    cursor = None
    try:
        cursor = connection.cursor(user=spec.user)
        metadata_columns = cursor.get_table_schema(
            _metadata_pattern(table), _metadata_pattern(database)
        )
        columns: List[Tuple[str, str]] = []
        for name, type_name in metadata_columns:
            if (
                not isinstance(name, str)
                or not name
                or not isinstance(type_name, str)
                or not type_name.strip()
            ):
                raise ValueError(
                    "HiveServer2 returned incomplete table column metadata"
                )
            columns.append((name, type_name))
        if any(type_name.strip().casefold() == "decimal" for _, type_name in columns):
            # Impyla's GetColumns wrapper drops DECIMAL precision and scale.
            # Hive's DESCRIBE result preserves them without reading table rows.
            cursor.execute(f"DESCRIBE `{database}`.`{table}`")
            described_types = {
                name.casefold(): type_name
                for name, type_name, *_ in cursor.fetchall()
                if name and type_name
            }
            columns = [
                (
                    name,
                    described_types.get(name.casefold(), type_name)
                    if type_name.strip().casefold() == "decimal"
                    else type_name,
                )
                for name, type_name in columns
            ]
    except Exception as exc:
        detail = _classify_error_detail(exc)
        if detail:
            raise RuntimeError(
                f"HiveServer2 table schema lookup failed: {detail}"
            ) from exc
        raise RuntimeError("HiveServer2 table schema lookup failed") from exc
    finally:
        if cursor is not None:
            with suppress(Exception):
                cursor.close()
        with suppress(Exception):
            connection.close()

    if not columns:
        raise ValueError("HiveServer2 table has no columns")
    unresolved = [
        name for name, type_name in columns if type_name.strip().casefold() == "decimal"
    ]
    if unresolved:
        raise ValueError(
            "HiveServer2 DECIMAL columns lack precision and scale: "
            + ", ".join(sorted(unresolved))
        )
    names = [name for name, _ in columns]
    if len({name.casefold() for name in names}) != len(names):
        raise ValueError("HiveServer2 table has duplicate column names")
    return pa.schema([(name, _arrow_type(type_name)) for name, type_name in columns])


def _statement(spec: HiveReadSpec) -> str:
    if spec.query is not None:
        return spec.query
    table_identifier = spec.table_identifier
    if table_identifier is None:
        raise ValueError("HiveServer2 statement requires a table or query")
    database, table = table_identifier
    statement = f"SELECT * FROM `{database}`.`{table}`"
    if spec.limit is not None:
        statement += f" LIMIT {spec.limit}"
    return statement


def _result_arrow_type(column) -> pa.DataType:
    """Map Impyla's result metadata to the supported Arrow scalar type."""
    try:
        type_name = column[1]
        if type_name.upper() == "DECIMAL":
            precision, scale = column[4], column[5]
            if any(
                isinstance(value, bool) or not isinstance(value, int)
                for value in (precision, scale)
            ):
                raise ValueError
            type_name = f"DECIMAL({precision},{scale})"
        return _arrow_type(type_name)
    except Exception as exc:
        raise ValueError("HiveServer2 result has an unsupported column type") from exc


def _check_result_schema(
    description, schema: pa.Schema, table_name: Optional[str] = None
) -> None:
    if description is None:
        raise ValueError("HiveServer2 statement did not return a result set")
    names = [column[0] for column in description]
    if table_name is not None:
        qualifier = f"{table_name}."
        names = [
            name[len(qualifier) :]
            if name.casefold().startswith(qualifier.casefold())
            else name
            for name in names
        ]
    if len(names) != len(schema) or any(
        actual.casefold() != expected.casefold()
        for actual, expected in zip(names, schema.names)
    ):
        raise ValueError("HiveServer2 result columns do not match the read schema")
    if any(
        _result_arrow_type(column) != field.type
        for column, field in zip(description, schema)
    ):
        raise ValueError("HiveServer2 result types do not match the read schema")


def _to_arrow(rows, schema: pa.Schema) -> pa.Table:
    try:
        arrays = [
            pa.array([row[index] for row in rows], type=field.type)
            for index, field in enumerate(schema)
        ]
    except Exception as exc:
        raise ValueError(
            "HiveServer2 rows could not be converted to the read schema"
        ) from exc
    if any(
        not field.nullable and array.null_count for field, array in zip(schema, arrays)
    ):
        raise ValueError("HiveServer2 rows violate a non-nullable schema field")
    try:
        return pa.Table.from_arrays(arrays, schema=schema)
    except Exception as exc:
        raise ValueError(
            "HiveServer2 rows could not be converted to the read schema"
        ) from exc


def read_hs2_batches(spec: HiveReadSpec, schema: pa.Schema) -> Iterator[pa.Table]:
    """Execute one statement and yield bounded Arrow batches."""
    if spec.limit == 0:
        yield pa.Table.from_batches([], schema=schema)
        return

    connection = _connect(spec.connection_factory)
    cursor = None
    try:
        try:
            cursor = connection.cursor(user=spec.user)
            cursor.execute(_statement(spec))
            description = cursor.description
        except Exception as exc:
            detail = _classify_error_detail(exc)
            if detail:
                raise RuntimeError(f"HiveServer2 read failed: {detail}") from exc
            raise RuntimeError("HiveServer2 read failed") from exc
        table_identifier = spec.table_identifier
        table_name = table_identifier[1] if table_identifier is not None else None
        _check_result_schema(description, schema, table_name)
        while True:
            try:
                rows = cursor.fetchmany(_FETCH_ROWS)
            except Exception as exc:
                detail = _classify_error_detail(exc)
                if detail:
                    raise RuntimeError(f"HiveServer2 read failed: {detail}") from exc
                raise RuntimeError("HiveServer2 read failed") from exc
            if not rows:
                break
            yield _to_arrow(rows, schema)
    finally:
        if cursor is not None:
            with suppress(Exception):
                cursor.cancel_operation()
            with suppress(Exception):
                cursor.close()
        with suppress(Exception):
            connection.close()
