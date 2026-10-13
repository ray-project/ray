"""Ray Serve application for the gRPC facade.

Deploys the gRPC facade (``grpc_facade``) behind Ray Serve's gRPC proxy, so
a Serve application, such as an Anyscale service, exposes the same API
without running the facade's own server. A Serve config names two entry
points:

* ``grpc_facade.add_servicers_to_server`` in
  ``grpc_options.grpc_servicer_functions``, which registers the client
  SDK's two services under their wire names.
* ``build_app`` from this module as the application's ``import_path``.

The ingress is the facade's own servicer, ``RaySandboxFacade``: Serve calls
the method named after each RPC, as a ``grpc.aio`` server does, so every
call passes the same token check. The facade keeps its exec table in
memory, so the deployment runs exactly one replica.

Requires ``ray[serve]``.
"""

import os
from typing import Any, Callable, Dict, Optional

from ray.experimental.sandbox.http.grpc_facade import RaySandboxFacade
from ray.experimental.sandbox.http.schemas import SandboxAPISettings
from ray.util.annotations import PublicAPI

_DEPLOYMENT_NAME = "RaySandboxGrpcFacade"
# Long polls (SandboxWait, TaskExecWait) and output streams each hold a slot
# for as long as they wait, so admit far more than Serve's default.
_MAX_ONGOING_REQUESTS = 10_000


class _FacadeIngress(RaySandboxFacade):
    """The facade as a Serve ingress, built in its replica."""

    def __init__(
        self,
        settings: SandboxAPISettings,
        advertise_url: Optional[str],
        handle_resolver_factory: Optional[Callable[[], Any]] = None,
    ) -> None:
        # The facade reads the token from this process's environment, which
        # can lack what build_app found where the application was built,
        # such as when `serve run` builds it outside the cluster.
        _require_token_env(settings)
        # Built in the replica: the resolver holds locks and actor handles,
        # which can't travel with the application.
        resolver = handle_resolver_factory() if handle_resolver_factory else None
        super().__init__(
            settings, handle_resolver=resolver, advertise_url=advertise_url
        )

    async def __call__(self, request: Any) -> Any:
        """Answer HTTP requests: the application serves only gRPC."""
        from starlette.responses import PlainTextResponse

        return PlainTextResponse("This application serves gRPC only.", status_code=404)


def _require_token_env(settings: SandboxAPISettings) -> None:
    """Refuse to serve without the API token in this process's environment.

    The facade serves every call unchecked without one, and Serve's proxies
    listen on every node's address, which sandboxes with network access can
    reach.
    """
    if not os.environ.get(settings.token_env_var):
        raise ValueError(
            f"set {settings.token_env_var} to the API token that clients must "
            "present: Serve's proxies listen on every node's address, which "
            "sandboxes with network access can reach"
        )


def _bind(
    settings: SandboxAPISettings,
    advertise_url: Optional[str],
    handle_resolver_factory: Optional[Callable[[], Any]] = None,
) -> Any:
    from ray import serve

    deployment = serve.deployment(
        _FacadeIngress,
        name=_DEPLOYMENT_NAME,
        num_replicas=1,
        max_ongoing_requests=_MAX_ONGOING_REQUESTS,
    )
    return deployment.bind(settings, advertise_url, handle_resolver_factory)


@PublicAPI(stability="alpha")
def build_app(args: Optional[Dict[str, Any]] = None) -> Any:
    """Ray Serve application builder for the gRPC facade.

    Use it as the ``import_path`` of an application whose Serve config also
    lists ``ray.experimental.sandbox.http.grpc_facade.add_servicers_to_server``
    in ``grpc_options.grpc_servicer_functions``.

    The facade requires the API token from the environment variable named
    by ``token_env_var`` (default ``RAY_SANDBOX_API_TOKEN``). Building the
    application fails without it, and so does starting its replica, which
    reads the variable from its own environment: Serve's proxies listen on
    every node's address, which sandboxes with network access can reach.

    Args:
        args: Builder arguments. ``advertise_url`` sets the command-router
            URL handed to clients (default: ``https://`` plus the host each
            client dialed). Every other key is a :class:`SandboxAPISettings`
            field.

    Returns:
        The facade's ingress deployment, bound and ready to run.
    """
    args = dict(args or {})
    advertise_url = args.pop("advertise_url", None)
    settings = SandboxAPISettings(**args)
    if settings.num_replicas != 1:
        raise ValueError(
            "the gRPC facade keeps its exec table in memory and runs as one "
            "replica; remove num_replicas"
        )
    _require_token_env(settings)
    return _bind(settings, advertise_url)
