import abc
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from google.cloud import bigquery, bigquery_storage

from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class BigQueryClientProvider(abc.ABC):
    """Abstract base class for constructing BigQuery clients.

    Subclass this to inject custom credentials or ``client_options`` into
    :func:`~ray.data.read_bigquery` and
    :meth:`~ray.data.Dataset.write_bigquery`.

    The *provider* — not the client — is the injectable unit because
    ``google.cloud`` clients are not picklable and cannot be shipped to
    Ray workers directly. A provider constructs clients lazily on each
    worker and must itself be picklable.

    Example:

    .. testcode::
        :skipif: True

        from ray.data._internal.datasource.bigquery_credentials import (
            BigQueryClientProvider,
        )

        class MyProvider(BigQueryClientProvider):
            def get_client(self, project_id: str):
                from google.cloud import bigquery
                from google.oauth2 import service_account
                creds = service_account.Credentials.from_service_account_file(
                    "/path/to/key.json"
                )
                return bigquery.Client(project=project_id, credentials=creds)

            def get_read_client(self, project_id: str):
                from google.cloud import bigquery_storage
                from google.oauth2 import service_account
                creds = service_account.Credentials.from_service_account_file(
                    "/path/to/key.json"
                )
                return bigquery_storage.BigQueryReadClient(credentials=creds)
    """

    @abc.abstractmethod
    def get_client(self, project_id: str) -> "bigquery.Client":
        """Return a ``bigquery.Client`` for control-plane operations.

        Args:
            project_id: The GCP project ID.

        Returns:
            A ``google.cloud.bigquery.Client`` instance.
        """

    @abc.abstractmethod
    def get_read_client(self, project_id: str) -> "bigquery_storage.BigQueryReadClient":
        """Return a ``BigQueryReadClient`` for data-plane reads.

        Args:
            project_id: The GCP project ID.

        Returns:
            A ``google.cloud.bigquery_storage.BigQueryReadClient`` instance.
        """


class _DefaultBigQueryClientProvider(BigQueryClientProvider):
    """Default provider — preserves existing behaviour.

    Args:
        credentials: A ``google.auth.credentials.Credentials`` instance.
            When ``None`` (default) Application Default Credentials are used.
        client_options: Optional client options passed to both clients.
        **client_kwargs: Extra kwargs forwarded to ``bigquery.Client()``.
    """

    def __init__(
        self,
        credentials: "Any" = None,
        client_options: "Any" = None,
        **client_kwargs: "Any",
    ) -> None:
        self._credentials = credentials
        self._client_options = client_options
        self._client_kwargs = client_kwargs

    def get_client(self, project_id: str):
        from google.cloud import bigquery

        from ray.data._internal.datasource.bigquery_datasource import (
            _create_client_info,
        )

        return bigquery.Client(
            project=project_id,
            credentials=self._credentials,
            client_options=self._client_options,
            client_info=_create_client_info(),
            **self._client_kwargs,
        )

    def get_read_client(self, project_id: str):
        from google.cloud import bigquery_storage

        from ray.data._internal.datasource.bigquery_datasource import (
            _create_client_info_gapic,
        )

        kwargs = {}
        if self._credentials is not None:
            kwargs["credentials"] = self._credentials
        if self._client_options is not None:
            kwargs["client_options"] = self._client_options

        return bigquery_storage.BigQueryReadClient(
            client_info=_create_client_info_gapic(),
            **kwargs,
        )
