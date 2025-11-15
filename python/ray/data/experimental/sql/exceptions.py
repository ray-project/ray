"""Exception classes for Ray Data SQL API.

This module defines the exception hierarchy used by the Ray Data SQL engine
to provide clear and actionable error messages for users.
"""

from typing import Optional


class SQLError(Exception):
    """Base exception class for all Ray Data SQL errors.

    This is the base class for all SQL-related exceptions in Ray Data.
    It provides common functionality for error handling and reporting.

    Args:
        message: Human-readable error message.
        query: The SQL query that caused the error, if available.
        line: Line number in the query where the error occurred, if available.
        column: Column number in the query where the error occurred, if available.
    """

    def __init__(
        self,
        message: str,
        query: Optional[str] = None,
        line: Optional[int] = None,
        column: Optional[int] = None,
    ):
        self.message = message
        self.query = query
        self.line = line
        self.column = column

        # Build comprehensive error message
        full_message = message
        if query:
            full_message += f"\nQuery: {query}"
        if line is not None:
            full_message += f"\nLine: {line}"
        if column is not None:
            full_message += f"\nColumn: {column}"

        super().__init__(full_message)


class SQLParseError(SQLError):
    """Exception raised when SQL query parsing fails.

    This exception is raised when the SQL query cannot be parsed due to
    syntax errors or unsupported SQL constructs.

    Examples:
        >>> try:
        ...     sql("SELECT * FROM")  # Incomplete query
        ... except SQLParseError as e:
        ...     print(f"Parse error: {e}")
    """

    pass


class SQLExecutionError(SQLError):
    """Exception raised during SQL query execution.

    This exception is raised when a valid SQL query fails during execution,
    typically due to runtime errors like type mismatches or data issues.

    Examples:
        >>> try:
        ...     sql("SELECT 1 / 0")  # Division by zero
        ... except SQLExecutionError as e:
        ...     print(f"Execution error: {e}")
    """

    pass


class TableNotFoundError(SQLError):
    """Exception raised when referencing a non-existent table.

    This exception is raised when a SQL query references a table that
    has not been registered with the SQL engine.

    Examples:
        >>> try:
        ...     sql("SELECT * FROM nonexistent_table")
        ... except TableNotFoundError as e:
        ...     print(f"Table not found: {e}")
    """

    def __init__(
        self,
        table_name: str,
        available_tables: Optional[list] = None,
        query: Optional[str] = None,
        line: Optional[int] = None,
        column: Optional[int] = None,
    ):
        """Initialize TableNotFoundError.

        Args:
            table_name: Name of the table that was not found.
            available_tables: List of available table names, if provided.
            query: The SQL query that caused the error, if provided.
            line: Line number in the query where the error occurred, if provided.
            column: Column number in the query where the error occurred, if provided.
        """
        self.table_name = table_name
        self.available_tables = available_tables or []

        message = f"Table '{table_name}' not found"
        if self.available_tables:
            message += f". Available tables: {', '.join(self.available_tables)}"

        super().__init__(message, query, line, column)


class ColumnNotFoundError(SQLError):
    """Exception raised when referencing a non-existent column.

    This exception is raised when a SQL query references a column that
    does not exist in the specified table.

    Examples:
        >>> try:
        ...     sql("SELECT nonexistent_column FROM users")
        ... except ColumnNotFoundError as e:
        ...     print(f"Column not found: {e}")
    """

    def __init__(
        self,
        column_name: str,
        table_name: Optional[str] = None,
        available_columns: Optional[list] = None,
        query: Optional[str] = None,
        line: Optional[int] = None,
        column: Optional[int] = None,
    ):
        """Initialize ColumnNotFoundError.

        Args:
            column_name: Name of the column that was not found.
            table_name: Name of the table where the column was expected.
            available_columns: List of available column names, if provided.
            query: The SQL query that caused the error, if provided.
            line: Line number in the query where the error occurred, if provided.
            column: Column number in the query where the error occurred, if provided.
        """
        self.column_name = column_name
        self.table_name = table_name
        self.available_columns = available_columns or []

        message = f"Column '{column_name}' not found"
        if table_name:
            message += f" in table '{table_name}'"
        if self.available_columns:
            message += f". Available columns: {', '.join(self.available_columns)}"

        super().__init__(message, query, line, column)


class UnsupportedOperationError(SQLError):
    """Exception raised when encountering unsupported SQL operations.

    This exception is raised when a SQL query contains operations or
    constructs that are not yet supported by the Ray Data SQL engine.

    Examples:
        >>> try:
        ...     sql("SELECT DISTINCT name FROM users")
        ... except UnsupportedOperationError as e:
        ...     print(f"Unsupported operation: {e}")
    """

    def __init__(
        self,
        operation: str,
        suggestion: Optional[str] = None,
        query: Optional[str] = None,
        line: Optional[int] = None,
        column: Optional[int] = None,
    ):
        """Initialize UnsupportedOperationError.

        Args:
            operation: Name of the unsupported operation.
            suggestion: Optional suggestion for alternative approaches.
            query: The SQL query that caused the error, if provided.
            line: Line number in the query where the error occurred, if provided.
            column: Column number in the query where the error occurred, if provided.
        """
        self.operation = operation
        self.suggestion = suggestion

        message = f"Unsupported operation: {operation}"
        if suggestion:
            message += f". {suggestion}"

        super().__init__(message, query, line, column)


class SchemaError(SQLError):
    """Exception raised for schema-related errors.

    This exception is raised when there are issues with data schemas,
    such as type mismatches or incompatible operations.

    Examples:
        >>> try:
        ...     sql("SELECT name + age FROM users")  # String + int
        ... except SchemaError as e:
        ...     print(f"Schema error: {e}")
    """

    pass
