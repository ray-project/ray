import logging
from typing import Optional

from ray.data._internal.datasource.sql_datasource import SQLDatasource
from ray.data._internal.util import _check_import

logger = logging.getLogger(__name__)

# PyHive documentation: https://github.com/dropbox/PyHive


class HiveDatasource(SQLDatasource):
    """
    Datasource for reading from Apache Hive using HiveServer2.

    This datasource extends SQLDatasource and uses PyHive for connectivity.
    PyHive is a Python DB API2-compliant driver for Hive and Presto.

    Args:
        host: HiveServer2 host address
        port: HiveServer2 port (default: 10000)
        database: Hive database name (default: "default")
        auth: Authentication mechanism - "NONE", "NOSASL", "KERBEROS", "LDAP", or "CUSTOM"
        username: Username for authentication (required for LDAP)
        password: Password for authentication (required for LDAP)
        configuration: Optional Hive configuration parameters
        kerberos_service_name: Kerberos service name (default: "hive")
        sql: SQL query to execute
        shard_keys: Optional column names for parallel reading

    References:
        - PyHive: https://github.com/dropbox/PyHive
        - HiveServer2: https://cwiki.apache.org/confluence/display/Hive/HiveServer2+Overview
        - Hive JDBC: https://cwiki.apache.org/confluence/display/Hive/HiveServer2+Clients
    """

    def __init__(
        self,
        *,
        host: str,
        port: int = 10000,
        database: str = "default",
        auth: str = "NONE",
        username: Optional[str] = None,
        password: Optional[str] = None,
        configuration: Optional[dict] = None,
        kerberos_service_name: str = "hive",
        sql: str,
        shard_keys: Optional[list[str]] = None,
        shard_hash_fn: str = "MD5",
    ):
        _check_import(self, module="pyhive", package="hive")

        # Store Hive connection parameters
        self._host = host
        self._port = port
        self._database = database
        self._auth = auth
        self._username = username
        self._password = password
        self._configuration = configuration or {}
        self._kerberos_service_name = kerberos_service_name

        # Create connection factory for PyHive
        def create_hive_connection():
            from pyhive import hive

            logger.debug(
                f"Connecting to Hive at {host}:{port}, database={database}, auth={auth}"
            )

            return hive.connect(
                host=self._host,
                port=self._port,
                database=self._database,
                auth=self._auth,
                username=self._username,
                password=self._password,
                configuration=self._configuration,
                kerberos_service_name=self._kerberos_service_name,
            )

        # Initialize parent SQLDatasource with the connection factory
        super().__init__(
            sql=sql,
            connection_factory=create_hive_connection,
            shard_hash_fn=shard_hash_fn,
            shard_keys=shard_keys,
        )
