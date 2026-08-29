"""FastAPI application and Ray Serve builder for the Ray Sandbox HTTP API.

Deploy on a Ray cluster whose worker nodes have gVisor's ``runsc`` on PATH:

    serve run ray.experimental.sandbox.http.app:build_app

or as an Anyscale service (see ``doc/source/ray-core/sandboxes.md``). Bearer
auth is enforced when the environment variable named by
``SandboxAPISettings.token_env_var`` (default ``RAY_SANDBOX_API_TOKEN``) is
set; an Anyscale service can leave it unset because the platform edge already
requires the service's own bearer token.

The service holds no state: every sandbox lives in a named detached
``SandboxHost`` actor and every replica resolves them by name, so replicas
can scale or restart freely.
"""

import asyncio
import hashlib
import hmac
import logging
import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Dict, List, Optional

from fastapi import APIRouter, Depends, FastAPI, Query, Request, Response
from fastapi.responses import JSONResponse

from ray.experimental.sandbox.http.host import SandboxHost
from ray.experimental.sandbox.http.schemas import (
    CreateSandboxRequest,
    ExecInfo,
    ExecStarted,
    SandboxAPISettings,
    SandboxInfo,
    SandboxList,
    StartExecRequest,
)
from ray.util.annotations import PublicAPI

logger = logging.getLogger(__name__)

SANDBOX_ID_PREFIX = "sb-"

_MAX_WAIT_SECONDS = 30.0

_WAIT_QUERY = Query(
    default=0.0,
    ge=0.0,
    le=_MAX_WAIT_SECONDS,
    description="Long-poll for up to this many seconds for a state change.",
)


class _ApiError(Exception):
    """Maps to the JSON error envelope; raised by handlers, caught app-wide."""

    def __init__(self, status_code: int, code: str, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message


def _sandbox_not_found(sandbox_id: str) -> _ApiError:
    return _ApiError(404, "sandbox_not_found", f"no sandbox with id {sandbox_id!r}")


class _SchedulingTimeout(_ApiError):
    """The sandbox's hosting actor has not been scheduled within the grace."""

    def __init__(self, sandbox_id: str) -> None:
        super().__init__(
            409,
            "conflict",
            f"sandbox {sandbox_id} is not ready yet (its actor is still "
            "being scheduled); retry shortly",
        )


def _is_unschedulable(exc: BaseException) -> bool:
    """True when Ray reports the actor can never fit the cluster's resources.

    Matched by class name for the same reason as ``_is_actor_gone``.
    """
    names = {type(exc).__name__, *(base.__name__ for base in type(exc).__mro__)}
    return any("ActorUnschedulableError" in name for name in names)


def _is_actor_gone(exc: BaseException) -> bool:
    """True when a remote call failed because the actor no longer exists.

    Matched by class name because Ray re-raises remote failures as
    dynamically-built subclasses, and so this module stays importable (and
    unit-testable) without a live Ray context.
    """
    names = {type(exc).__name__, *(base.__name__ for base in type(exc).__mro__)}
    return any(
        "RayActorError" in name
        or "ActorDiedError" in name
        or "ActorUnavailableError" in name
        for name in names
    )


async def _actor_call(sandbox_id: str, awaitable: Awaitable[Any]) -> Any:
    """Await a SandboxHost call, mapping a dead actor to 404."""
    try:
        return await awaitable
    except Exception as exc:
        if _is_actor_gone(exc):
            raise _sandbox_not_found(sandbox_id) from exc
        if _is_unschedulable(exc):
            # The requested cpu/memory shape cannot fit any node the cluster
            # can offer: a permanent condition, not a scheduling wait.
            raise _ApiError(
                409,
                "unschedulable",
                f"sandbox {sandbox_id} cannot be scheduled: {str(exc)[:300]}",
            ) from exc
        raise


def _sandbox_name_for_token(client_token: str) -> str:
    digest = hashlib.sha256(client_token.encode("utf-8")).hexdigest()
    return f"{SANDBOX_ID_PREFIX}{digest[:12]}"


def _parse_label_filters(labels: List[str]) -> Dict[str, str]:
    filters: Dict[str, str] = {}
    for item in labels:
        key, sep, value = item.partition("=")
        if not sep or not key:
            raise _ApiError(
                400,
                "invalid_request",
                f"label filter {item!r} must have the form key=value",
            )
        filters[key] = value
    return filters


@PublicAPI(stability="alpha")
class RayActorHandleResolver:
    """Creates and resolves the named detached SandboxHost actors.

    The default (and only production) resolver; tests inject a fake with the
    same four methods so the HTTP layer runs without a Ray cluster.
    """

    def __init__(self, settings: SandboxAPISettings) -> None:
        self._settings = settings

    def create(
        self,
        name: str,
        actor_options: Dict[str, Any],
        ctor_kwargs: Dict[str, Any],
    ) -> Any:
        import ray

        return (
            ray.remote(SandboxHost)
            .options(
                name=name,
                namespace=self._settings.namespace,
                lifetime="detached",
                # Atomic get-or-create: two replicas racing on the same
                # client_token converge on one actor. boot() is idempotent.
                get_if_exists=True,
                **actor_options,
            )
            .remote(**ctor_kwargs)
        )

    def get(self, name: str) -> Optional[Any]:
        import ray

        try:
            return ray.get_actor(name, namespace=self._settings.namespace)
        except ValueError:
            return None

    def list_names(self) -> List[str]:
        from ray.util import list_named_actors

        names: List[str] = []
        for entry in list_named_actors(all_namespaces=True):
            if entry.get("namespace") == self._settings.namespace and entry.get(
                "name", ""
            ).startswith(SANDBOX_ID_PREFIX):
                names.append(entry["name"])
        return names

    def kill(self, handle: Any) -> None:
        import ray

        try:
            ray.kill(handle)
        except Exception as exc:
            logger.debug("Failed to kill sandbox actor: %s", exc)


@PublicAPI(stability="alpha")
def create_app(
    settings: Optional[SandboxAPISettings] = None,
    *,
    handle_resolver: Optional[Any] = None,
) -> FastAPI:
    """Build the FastAPI app.

    Args:
        settings: Server settings; defaults are production-safe.
        handle_resolver: Test seam; anything with the
            ``RayActorHandleResolver`` method surface. Defaults to the real
            Ray-backed resolver.

    Returns:
        The configured FastAPI application.
    """
    settings = settings or SandboxAPISettings()
    resolver = handle_resolver or RayActorHandleResolver(settings)
    token = os.environ.get(settings.token_env_var) or None

    async def _bounded_call(
        sandbox_id: str, awaitable: Awaitable[Any], extra_wait: float = 0.0
    ) -> Any:
        """Await a SandboxHost call, but never block behind actor scheduling.

        A named detached actor exists the moment it is created, but on a
        cluster that is scaling up it may not be *scheduled* for a while —
        and a call to an unscheduled actor blocks indefinitely. Cap the wait
        at the request's own long-poll budget plus a grace period.
        """
        try:
            return await asyncio.wait_for(
                _actor_call(sandbox_id, awaitable),
                timeout=extra_wait + settings.scheduling_grace_seconds,
            )
        except asyncio.TimeoutError:
            raise _SchedulingTimeout(sandbox_id)

    async def _describe_or_pending(
        sandbox_id: str, handle: Any, wait_seconds: float = 0.0
    ) -> Dict[str, Any]:
        try:
            return await _bounded_call(
                sandbox_id,
                handle.describe.remote(wait_seconds=wait_seconds),
                extra_wait=wait_seconds,
            )
        except _SchedulingTimeout:
            # The actor owns the spec, so report a shape-complete pending
            # info with the fields only it knows left empty.
            return {
                "sandbox_id": sandbox_id,
                "status": "pending",
                "image": "",
                "created_at": datetime.now(timezone.utc).isoformat(),
                "ttl_seconds": None,
                "expires_at": None,
                "network": "none",
                "labels": {},
                "error": None,
            }

    async def require_bearer_token(request: Request) -> None:
        if token is None:
            return
        provided = request.headers.get("authorization", "")
        if not hmac.compare_digest(provided, f"Bearer {token}"):
            raise _ApiError(401, "unauthorized", "invalid or missing bearer token")

    public = APIRouter(prefix="/api/v1")
    v1 = APIRouter(prefix="/api/v1", dependencies=[Depends(require_bearer_token)])

    @public.get("/health")
    async def health() -> Dict[str, str]:
        return {"status": "ok"}

    # ------------------------------------------------------------------
    # Sandboxes
    # ------------------------------------------------------------------

    @v1.post("/sandboxes", response_model=SandboxInfo, status_code=202)
    async def create_sandbox(request: CreateSandboxRequest) -> JSONResponse:
        if (
            request.ttl_seconds is not None
            and request.ttl_seconds > settings.max_ttl_seconds
        ):
            raise _ApiError(
                400,
                "invalid_request",
                f"ttl_seconds may not exceed {settings.max_ttl_seconds}",
            )
        # No TTL still gets the server-wide bound so abandoned sandboxes are
        # always reclaimed eventually.
        effective_ttl = (
            request.ttl_seconds
            if request.ttl_seconds is not None
            else settings.max_ttl_seconds
        )

        if request.client_token is not None:
            sandbox_id = _sandbox_name_for_token(request.client_token)
            existing = resolver.get(sandbox_id)
            if existing is not None:
                try:
                    info = await _describe_or_pending(sandbox_id, existing)
                    return JSONResponse(status_code=200, content=info)
                except _ApiError as exc:
                    if exc.code != "sandbox_not_found":
                        raise
                    # The previous actor died (node loss, OOM). Clear it and
                    # fall through to create a fresh sandbox under the same
                    # name, keeping the token idempotent across failures.
                    resolver.kill(existing)
        else:
            sandbox_id = f"{SANDBOX_ID_PREFIX}{uuid.uuid4().hex[:12]}"

        resources = request.resources
        cpu_request = resources.cpu_request if resources else None
        cpu_limit = resources.cpu_limit if resources else None
        memory_request_mb = resources.memory_request_mb if resources else None
        memory_limit_mb = resources.memory_limit_mb if resources else None
        # A capped sandbox should also be scheduled onto capacity that can
        # honor the cap, so requests default to the limits.
        if cpu_request is None:
            cpu_request = cpu_limit
        if memory_request_mb is None:
            memory_request_mb = memory_limit_mb

        actor_options: Dict[str, Any] = {
            "num_cpus": (
                cpu_request
                if cpu_request is not None
                else settings.default_actor_num_cpus
            ),
        }
        if memory_request_mb is not None:
            actor_options["memory"] = memory_request_mb * 1024 * 1024
        if resources is not None and resources.custom:
            actor_options["resources"] = dict(resources.custom)

        capabilities = (
            request.capabilities
            if request.capabilities is not None
            else list(settings.default_capabilities)
        )
        spec = {
            "image": request.image,
            "env": request.env,
            "workdir": request.workdir,
            "ttl_seconds": effective_ttl,
            "network": request.network,
            "dns": request.dns,
            "shell": request.shell,
            "rootless": request.rootless,
            "readonly": request.readonly,
            "capabilities": capabilities,
            "cpu_limit": cpu_limit,
            "memory_limit_mb": memory_limit_mb,
            "image_pull_timeout_seconds": request.image_pull_timeout_seconds,
            "start_timeout_seconds": request.start_timeout_seconds,
            "labels": request.labels,
        }
        host_settings = {
            "max_output_bytes": settings.max_output_bytes,
            "max_exec_history": settings.max_exec_history,
            "auto_install_runsc": settings.auto_install_runsc,
        }
        handle = resolver.create(
            sandbox_id,
            actor_options,
            {
                "sandbox_id": sandbox_id,
                "spec": spec,
                "settings": host_settings,
            },
        )
        # Fire-and-forget: boot progress and failures are reported through
        # describe(), never through this call's result. The response is
        # synthesized from the request rather than fetched from the actor so
        # that creation never blocks behind actor scheduling — on a saturated
        # cluster the new actor may legitimately be queued for a while.
        handle.boot.remote()
        created_at = datetime.now(timezone.utc)
        info = {
            "sandbox_id": sandbox_id,
            "status": "pending",
            "image": request.image,
            "created_at": created_at.isoformat(),
            "ttl_seconds": effective_ttl,
            "expires_at": (created_at + timedelta(seconds=effective_ttl)).isoformat(),
            "network": request.network,
            "labels": request.labels,
            "error": None,
        }
        return JSONResponse(status_code=202, content=info)

    @v1.get("/sandboxes", response_model=SandboxList)
    async def list_sandboxes(
        label: List[str] = Query(default=[]),
    ) -> Dict[str, Any]:
        filters = _parse_label_filters(label)
        named = [(name, resolver.get(name)) for name in resolver.list_names()]
        # Concurrently: sequential describes would make the listing scale
        # linearly with the number of sandboxes.
        results = await asyncio.gather(
            *(
                _describe_or_pending(name, handle)
                for name, handle in named
                if handle is not None
            ),
            return_exceptions=True,
        )
        sandboxes: List[Dict[str, Any]] = []
        for info in results:
            if isinstance(info, _ApiError) and info.code == "sandbox_not_found":
                continue
            if isinstance(info, BaseException):
                raise info
            if all(info.get("labels", {}).get(k) == v for k, v in filters.items()):
                sandboxes.append(info)
        return {"sandboxes": sandboxes}

    @v1.get("/sandboxes/{sandbox_id}", response_model=SandboxInfo)
    async def get_sandbox(
        sandbox_id: str, wait_seconds: float = _WAIT_QUERY
    ) -> Dict[str, Any]:
        handle = resolver.get(sandbox_id)
        if handle is None:
            raise _sandbox_not_found(sandbox_id)
        return await _describe_or_pending(sandbox_id, handle, wait_seconds)

    @v1.delete("/sandboxes/{sandbox_id}")
    async def delete_sandbox(sandbox_id: str) -> Dict[str, str]:
        handle = resolver.get(sandbox_id)
        if handle is not None:
            try:
                await handle.terminate.remote()
            except Exception as exc:
                if not _is_actor_gone(exc):
                    raise
            resolver.kill(handle)
        # Idempotent: deleting an unknown or already-gone sandbox succeeds.
        return {"sandbox_id": sandbox_id, "status": "terminated"}

    # ------------------------------------------------------------------
    # Execs
    # ------------------------------------------------------------------

    @v1.post(
        "/sandboxes/{sandbox_id}/execs",
        response_model=ExecStarted,
        status_code=202,
    )
    async def start_exec(sandbox_id: str, request: StartExecRequest) -> Dict[str, Any]:
        if (
            request.timeout_seconds is not None
            and request.timeout_seconds > settings.max_exec_timeout_seconds
        ):
            raise _ApiError(
                400,
                "invalid_request",
                f"timeout_seconds may not exceed {settings.max_exec_timeout_seconds}",
            )
        handle = resolver.get(sandbox_id)
        if handle is None:
            raise _sandbox_not_found(sandbox_id)
        result = await _bounded_call(
            sandbox_id,
            handle.start_exec.remote(
                command=request.command,
                cwd=request.cwd,
                env=request.env,
                timeout_seconds=request.timeout_seconds,
                shell=request.shell,
                user=request.user,
            ),
        )
        if result.get("error_code") == "conflict":
            raise _ApiError(409, "conflict", result["message"])
        return result

    @v1.get("/sandboxes/{sandbox_id}/execs/{exec_id}", response_model=ExecInfo)
    async def get_exec(
        sandbox_id: str, exec_id: str, wait_seconds: float = _WAIT_QUERY
    ) -> Dict[str, Any]:
        handle = resolver.get(sandbox_id)
        if handle is None:
            raise _sandbox_not_found(sandbox_id)
        result = await _bounded_call(
            sandbox_id,
            handle.get_exec.remote(exec_id, wait_seconds=wait_seconds),
            extra_wait=wait_seconds,
        )
        if result.get("error_code") == "exec_not_found":
            raise _ApiError(404, "exec_not_found", result["message"])
        return result

    # ------------------------------------------------------------------
    # Files
    # ------------------------------------------------------------------

    def _validate_file_path(path: str) -> None:
        if not path.startswith("/"):
            raise _ApiError(400, "invalid_request", "file path must be absolute")

    @v1.put("/sandboxes/{sandbox_id}/files", status_code=204)
    async def put_file(
        sandbox_id: str,
        request: Request,
        path: str = Query(min_length=1),
        append: bool = Query(
            default=False,
            description=(
                "Append to the file instead of truncating it. Lets clients "
                "chunk large uploads under proxy body-size limits."
            ),
        ),
    ) -> Response:
        _validate_file_path(path)
        body = await request.body()
        if len(body) > settings.max_file_bytes:
            raise _ApiError(
                413,
                "payload_too_large",
                f"file body may not exceed {settings.max_file_bytes} bytes",
            )
        handle = resolver.get(sandbox_id)
        if handle is None:
            raise _sandbox_not_found(sandbox_id)
        result = await _bounded_call(
            sandbox_id, handle.write_file.remote(path, body, append=append)
        )
        if result.get("error_code") == "conflict":
            raise _ApiError(409, "conflict", result["message"])
        return Response(status_code=204)

    @v1.get("/sandboxes/{sandbox_id}/files")
    async def get_file(sandbox_id: str, path: str = Query(min_length=1)) -> Response:
        _validate_file_path(path)
        handle = resolver.get(sandbox_id)
        if handle is None:
            raise _sandbox_not_found(sandbox_id)
        result = await _bounded_call(sandbox_id, handle.read_file.remote(path))
        if result.get("error_code") == "conflict":
            raise _ApiError(409, "conflict", result["message"])
        if result.get("error_code") == "file_not_found":
            raise _ApiError(404, "file_not_found", result["message"])
        return Response(
            content=result["content"], media_type="application/octet-stream"
        )

    app = FastAPI(title="Ray Sandbox API", version="1.0.0")
    app.include_router(public)
    app.include_router(v1)

    @app.exception_handler(_ApiError)
    async def api_error_handler(request: Request, exc: _ApiError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code,
            content={"error": {"code": exc.code, "message": exc.message}},
        )

    @app.exception_handler(Exception)
    async def internal_error_handler(request: Request, exc: Exception) -> JSONResponse:
        logger.exception("Unhandled error serving %s", request.url.path)
        return JSONResponse(
            status_code=500,
            content={"error": {"code": "internal", "message": "internal server error"}},
        )

    return app


@PublicAPI(stability="alpha")
def build_app(args: Optional[Dict[str, Any]] = None) -> Any:
    """Ray Serve application builder.

    Usable directly with ``serve run`` (builder args become
    :class:`SandboxAPISettings` fields)::

        serve run ray.experimental.sandbox.http.app:build_app

    or from an Anyscale service config via ``import_path``.
    """
    from ray import serve

    settings = SandboxAPISettings(**(args or {}))
    fastapi_app = create_app(settings)

    # The API is long-poll based (requests deliberately hold a slot for up
    # to ~30s), so the replica must admit far more concurrent requests than
    # Serve's default allows.
    @serve.deployment(name="RaySandboxAPI", max_ongoing_requests=1000)
    @serve.ingress(fastapi_app)
    class SandboxAPIIngress:
        pass

    return SandboxAPIIngress.options(num_replicas=settings.num_replicas).bind()
