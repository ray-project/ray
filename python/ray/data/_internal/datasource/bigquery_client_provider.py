"""BigQuery client providers for Ray Data.

This module provides client abstraction for the Google Cloud clients used by
:func:`ray.data.read_bigquery` and :meth:`ray.data.Dataset.write_bigquery`,
allowing callers to supply credentials, endpoints, and other client options
instead of relying on Application Default Credentials.
"""

import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Dict, Optional

from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    from google.api_core.client_options import ClientOptions
    from google.auth.credentials import Credentials
    from google.cloud import bigquery, bigquery_storage

logger = logging.getLogger(__name__)


def _create_user_agent() -> str:
    import ray

    return f"ray/{ray.__version__}"


def _create_client_info():
    from google.api_core.client_info import ClientInfo

    return ClientInfo(
        user_agent=_create_user_agent(),
    )


def _create_client_info_gapic():
    from google.api_core.gapic_v1.client_info import ClientInfo

    return ClientInfo(
        user_agent=_create_user_agent(),
    )


@PublicAPI(stability="alpha")
class BigQueryClientProvider(ABC):
    """Abstract base class for providing BigQuery clients.

    Ray Data uses two Google Cloud clients for BigQuery: a ``bigquery.Client``
    for control-plane calls (running queries, creating datasets, loading
    tables) and a ``bigquery_storage.BigQueryReadClient`` for the bulk reads
    performed by each read task. Implement this class to control how both are
    constructed -- for example, to supply explicit credentials, a regional
    endpoint, or a custom retry policy.

    Subclasses must implement:
        - ``_build_client()``: Constructs the ``bigquery.Client``.
        - ``_build_read_client()``: Constructs the ``BigQueryReadClient``.

    Provider instances are pickled and shipped to Ray workers, so they must be
    serializable. Google's clients are *not* serializable, which is why the
    provider -- rather than a client instance -- is the injected unit. Build
    clients inside ``_build_client`` / ``_build_read_client`` rather than in
    ``__init__``; clients cached by this base class are dropped from the
    pickled state and rebuilt on the worker.

    Example:
        .. testcode::
            :skipif: True

            import ray
            from ray.data import BigQueryClientProvider

            class ImpersonatedClientProvider(BigQueryClientProvider):
                def __init__(self, project_id, service_account):
                    self._project_id = project_id
                    self._service_account = service_account

                def _credentials(self):
                    import google.auth
                    from google.auth import impersonated_credentials

                    source, _ = google.auth.default()
                    return impersonated_credentials.Credentials(
                        source_credentials=source,
                        target_principal=self._service_account,
                        target_scopes=["https://www.googleapis.com/auth/bigquery"],
                    )

                def _build_client(self):
                    from google.cloud import bigquery

                    return bigquery.Client(
                        project=self._project_id, credentials=self._credentials()
                    )

                def _build_read_client(self):
                    from google.cloud import bigquery_storage

                    return bigquery_storage.BigQueryReadClient(
                        credentials=self._credentials()
                    )

            ds = ray.data.read_bigquery(
                project_id="my_project",
                dataset="my_dataset.my_table",
                client_provider=ImpersonatedClientProvider(
                    "my_project", "svc@my_project.iam.gserviceaccount.com"
                ),
            )
    """

    def get_client(self) -> "bigquery.Client":
        """Get the BigQuery control-plane client, building it on first use.

        Returns:
            A ``google.cloud.bigquery.Client``.
        """
        client = getattr(self, "_cached_client", None)
        if client is None:
            client = self._build_client()
            self._cached_client = client
        return client

    def get_read_client(self) -> "bigquery_storage.BigQueryReadClient":
        """Get the BigQuery Storage Read client, building it on first use.

        Returns:
            A ``google.cloud.bigquery_storage.BigQueryReadClient``.
        """
        client = getattr(self, "_cached_read_client", None)
        if client is None:
            client = self._build_read_client()
            self._cached_read_client = client
        return client

    @abstractmethod
    def _build_client(self) -> "bigquery.Client":
        """Construct a new BigQuery control-plane client."""
        ...

    @abstractmethod
    def _build_read_client(self) -> "bigquery_storage.BigQueryReadClient":
        """Construct a new BigQuery Storage Read client."""
        ...

    def __getstate__(self) -> Dict[str, Any]:
        # Google's clients aren't serializable, so cached clients are dropped
        # here and lazily rebuilt by whichever process unpickles the provider.
        state = self.__dict__.copy()
        state.pop("_cached_client", None)
        state.pop("_cached_read_client", None)
        return state


@PublicAPI(stability="alpha")
class DefaultBigQueryClientProvider(BigQueryClientProvider):
    """The client provider used when none is supplied.

    Builds clients the way Ray Data always has -- Application Default
    Credentials, tagged with a Ray user agent -- while allowing explicit
    credentials and client options to be passed through.

    Args:
        project_id: The Google Cloud project the control-plane client bills to.
            If ``None``, the client infers the project from the environment.
        credentials: Credentials to use for both clients. If ``None``,
            Application Default Credentials are used. Credentials are pickled
            along with this provider, so they must be serializable; if yours
            aren't, subclass :class:`BigQueryClientProvider` and construct them
            in ``_build_client`` instead.
        client_options: ``google.api_core.client_options.ClientOptions``
            applied to both clients, for example to set ``api_endpoint`` or
            ``quota_project_id``.
        client_kwargs: Additional keyword arguments passed to
            ``bigquery.Client``. Takes precedence over the arguments above.
        read_client_kwargs: Additional keyword arguments passed to
            ``bigquery_storage.BigQueryReadClient``. Takes precedence over the
            arguments above.
    """

    def __init__(
        self,
        project_id: Optional[str] = None,
        *,
        credentials: Optional["Credentials"] = None,
        client_options: Optional["ClientOptions"] = None,
        client_kwargs: Optional[Dict[str, Any]] = None,
        read_client_kwargs: Optional[Dict[str, Any]] = None,
    ):
        self._project_id = project_id
        self._credentials = credentials
        self._client_options = client_options
        self._client_kwargs = dict(client_kwargs or {})
        self._read_client_kwargs = dict(read_client_kwargs or {})

    def _common_kwargs(self, overrides: Dict[str, Any]) -> Dict[str, Any]:
        kwargs = dict(overrides)
        if self._credentials is not None:
            kwargs.setdefault("credentials", self._credentials)
        if self._client_options is not None:
            kwargs.setdefault("client_options", self._client_options)
        return kwargs

    def _build_client(self) -> "bigquery.Client":
        from google.cloud import bigquery

        kwargs = self._common_kwargs(self._client_kwargs)
        kwargs.setdefault("project", self._project_id)
        kwargs.setdefault("client_info", _create_client_info())
        return bigquery.Client(**kwargs)

    def _build_read_client(self) -> "bigquery_storage.BigQueryReadClient":
        from google.cloud import bigquery_storage

        kwargs = self._common_kwargs(self._read_client_kwargs)
        kwargs.setdefault("client_info", _create_client_info_gapic())
        return bigquery_storage.BigQueryReadClient(**kwargs)


def resolve_client_provider(
    client_provider: Optional[BigQueryClientProvider],
    project_id: Optional[str],
) -> BigQueryClientProvider:
    """Return ``client_provider``, or the default provider if it's ``None``."""
    if client_provider is not None:
        return client_provider
    return DefaultBigQueryClientProvider(project_id=project_id)
