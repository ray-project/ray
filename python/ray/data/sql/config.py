"""Configuration for Ray Data SQL API."""

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Optional

from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class LogLevel(Enum):
    """Logging levels for the SQL engine.

    Attributes:
        DEBUG: Detailed debug information.
        INFO: General information about operations.
        WARNING: Warning messages for potential issues.
        ERROR: Error messages for failures.
    """

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"


@PublicAPI(stability="alpha")
class SQLDialect(Enum):
    """SQL dialects supported by the Ray Data SQL engine.

    Attributes:
        DUCKDB: DuckDB SQL dialect (default, most permissive).
        POSTGRES: PostgreSQL SQL dialect.
        MYSQL: MySQL SQL dialect.
        SQLITE: SQLite SQL dialect.
        SPARK: Apache Spark SQL dialect.
        BIGQUERY: Google BigQuery SQL dialect.
        SNOWFLAKE: Snowflake SQL dialect.
        REDSHIFT: Amazon Redshift SQL dialect.
    """

    DUCKDB = "duckdb"
    POSTGRES = "postgres"
    MYSQL = "mysql"
    SQLITE = "sqlite"
    SPARK = "spark"
    BIGQUERY = "bigquery"
    SNOWFLAKE = "snowflake"
    REDSHIFT = "redshift"

    def to_logging_level(self) -> int:
        """Convert to Python logging level.

        Returns:
            The corresponding Python logging level constant.
        """
        mapping = {
            LogLevel.DEBUG: logging.DEBUG,
            LogLevel.INFO: logging.INFO,
            LogLevel.WARNING: logging.WARNING,
            LogLevel.ERROR: logging.ERROR,
        }
        return mapping[self]


@PublicAPI(stability="alpha")
@dataclass
class SQLConfig:
    """Configuration for Ray Data SQL engine.

    Examples:
        >>> config = SQLConfig(log_level=LogLevel.DEBUG)
        >>> engine = RaySQL(config)
    """

    log_level: LogLevel = LogLevel.INFO
    dialect: SQLDialect = SQLDialect.DUCKDB
    case_sensitive: bool = True
    strict_mode: bool = False
    enable_optimization: bool = True
    max_join_partitions: int = 20
    enable_predicate_pushdown: bool = True
    enable_projection_pushdown: bool = True
    query_timeout_seconds: Optional[int] = None
    enable_sqlglot_optimizer: bool = False

    def __post_init__(self):
        """Validate configuration parameters."""
        if self.max_join_partitions <= 0:
            raise ValueError("max_join_partitions must be positive")

        if self.query_timeout_seconds is not None and self.query_timeout_seconds <= 0:
            raise ValueError("query_timeout_seconds must be positive")

    def get_logger(self, name: str = "ray.data.sql") -> logging.Logger:
        """Get a configured logger for the SQL engine.

        Args:
            name: Logger name.

        Returns:
            Configured logger instance.
        """
        logger = logging.getLogger(name)
        logger.setLevel(self.log_level.to_logging_level())

        # Add handler if none exists
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        return logger


# Default configuration instance
DEFAULT_CONFIG = SQLConfig()
