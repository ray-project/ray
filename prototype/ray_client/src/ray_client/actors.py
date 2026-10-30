"""ActorHandle and method proxies for stateful root actors."""

import urllib.parse
from typing import TYPE_CHECKING, Any, Dict, Iterator, Optional

from ray_client.packaging import to_json

if TYPE_CHECKING:
    from ray_client.client import RayCluster


class ActorHandle:
    """Handle to a stateful root actor managed through `/api/actors`.

    `ActorHandle` proxies method calls over HTTP/JSON to a remote actor process on the
    Ray cluster. Synchronous and `async def` methods return decoded JSON values, while
    generator and async generator methods return an iterator of Server-Sent Events (SSE).
    Using `ActorHandle` as a context manager (`with cluster.actor(...) as handle:`)
    automatically terminates the actor when exiting the `with` block.

    Args:
        name: Unique actor name registered in `/api/actors`.
        cluster: Owning `RayCluster` client instance.
        created: Whether this handle created a new actor (`True`) or reattached to an
            existing actor (`False`).
        package_uploaded: Whether a local `working_dir` archive was uploaded to the
            cluster during creation.
    """

    def __init__(
        self,
        name: str,
        cluster: "RayCluster",
        *,
        created: bool = False,
        package_uploaded: bool = False,
    ) -> None:
        if not isinstance(name, str) or not name.strip():
            raise ValueError("Actor name must be a non-empty string.")
        self._name = name
        self._cluster = cluster
        self._created = created
        self._package_uploaded = package_uploaded

    def _quoted_name(self) -> str:
        return urllib.parse.quote(self._name, safe="")

    def __repr__(self) -> str:
        return f"ActorHandle(name={self._name!r}, address={self._cluster.address!r})"

    @property
    def name(self) -> str:
        """Name of the actor on the cluster."""
        return self._name

    @property
    def created(self) -> bool:
        """Whether this handle created a new actor (`True`) or reattached (`False`)."""
        return self._created

    @property
    def package_uploaded(self) -> bool:
        """Whether creating this actor uploaded a new `working_dir` package."""
        return self._package_uploaded

    def info(self) -> Dict[str, Any]:
        """Fetches current status and metadata for the actor from `/api/actors/{name}`.

        Returns:
            Dictionary describing the actor's status, class name, active call count,
            spec hash, and remaining idle TTL.

        Raises:
            RayClientError: If the actor does not exist or the HTTP request fails.
        """
        return self._cluster._json("GET", f"/api/actors/{self._quoted_name()}")

    def logs(self) -> str:
        """Fetches captured stdout and stderr output from the root actor process.

        Returns:
            Captured log text from the actor process.

        Raises:
            RayClientError: If the actor does not exist or the HTTP request fails.
        """
        resp = self._cluster._json("GET", f"/api/actors/{self._quoted_name()}/logs")
        if isinstance(resp, dict):
            return str(resp.get("logs", ""))
        return str(resp)

    def _invoke(
        self,
        method: str,
        args: tuple,
        kwargs: Optional[Dict[str, Any]],
        timeout: Optional[float],
        *,
        stream: bool = False,
    ) -> Any:
        if not isinstance(method, str) or not method.strip():
            raise ValueError("Actor method name must be a non-empty string.")
        to_json(list(args), f"Actor '{self._name}.{method}' positional arguments")
        to_json(dict(kwargs or {}), f"Actor '{self._name}.{method}' keyword arguments")
        body: Dict[str, Any] = {
            "args": list(args),
            "kwargs": dict(kwargs or {}),
        }
        if stream:
            body["stream"] = True
        q_method = urllib.parse.quote(method, safe="")
        return self._cluster._call_or_stream_actor_method(
            f"/api/actors/{self._quoted_name()}/methods/{q_method}",
            body,
            target=f"{self._name}.{method}",
            submission_id=f"{self._name}.{method}",
            timeout=timeout,
            is_actor_method=True,
            require_stream=stream,
        )

    def call(
        self,
        method: str,
        *args: Any,
        kwargs: Optional[Dict[str, Any]] = None,
        timeout: Optional[float] = None,
    ) -> Any:
        """Invokes `method(*args, **kwargs)` on the remote actor.

        Args:
            method: Name of the public method (or `"__call__"`) to invoke on the actor.
            *args: Positional arguments serialized as round-trip JSON.
            kwargs: Optional dictionary of keyword arguments serialized as JSON.
            timeout: Optional timeout in seconds for the method call or SSE stream.

        Returns:
            The decoded JSON return value for a synchronous or `async def` method, or
            an SSE iterator of yielded JSON items if the method is a generator or async
            generator.

        Raises:
            RayClientSerializationError: If `args` or `kwargs` are not round-trip JSON.
            RayJobExecutionError: If the remote method raises an exception or returns a
                non-JSON value.
            RayJobTimeoutError: If the call exceeds `timeout`.
            RayClientError: If the actor does not exist or the HTTP request fails.
        """
        return self._invoke(method, args, kwargs, timeout)

    def stream(
        self,
        method: str,
        *args: Any,
        kwargs: Optional[Dict[str, Any]] = None,
        timeout: Optional[float] = None,
    ) -> Iterator[Any]:
        """Streams yielded JSON items from generator `method(*args, **kwargs)` over SSE.

        Args:
            method: Name of the generator or async generator method on the actor.
            *args: Positional arguments serialized as round-trip JSON.
            kwargs: Optional dictionary of keyword arguments serialized as JSON.
            timeout: Optional timeout in seconds for the SSE stream.

        Returns:
            An iterator of decoded JSON items yielded by the remote generator.

        Raises:
            RayClientSerializationError: If `args` or `kwargs` are not round-trip JSON.
            RayJobExecutionError: If the target method is not a generator or raises an
                exception during execution.
            RayJobTimeoutError: If the stream exceeds `timeout`.
            RayClientError: If the HTTP request fails or does not return an SSE stream.
        """
        return self._invoke(method, args, kwargs, timeout, stream=True)

    def __enter__(self) -> "ActorHandle":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.kill()

    def kill(self) -> bool:
        """Permanently terminates the actor and all child resources it owns.

        Returns:
            `True` if the actor existed and was terminated, or `False` if the actor was
            already gone (HTTP 404).

        Raises:
            RayClientError: If the `DELETE /api/actors/{name}` request fails.
        """
        endpoint = f"/api/actors/{self._quoted_name()}"
        resp = self._cluster.request("DELETE", endpoint)
        if resp.status_code == 404:
            return False
        if resp.status_code != 200:
            raise self._cluster._http_error("DELETE", endpoint, resp)
        return bool(resp.json().get("killed", True))

    def __getattr__(self, method: str) -> "_ActorMethodProxy":
        if method.startswith("_"):
            raise AttributeError(method)
        return _ActorMethodProxy(self, method)


class _ActorMethodProxy:
    """Bound method proxy on `ActorHandle` supporting `actor.method(...)`, `.call()`, and `.stream()`."""

    def __init__(self, actor: ActorHandle, method: str) -> None:
        self._actor = actor
        self._method = method
        self.__name__ = method

    def __repr__(self) -> str:
        return f"ActorMethodProxy(actor={self._actor.name!r}, method={self._method!r})"

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self._actor.call(self._method, *args, kwargs=kwargs or None)

    def call(
        self,
        *args: Any,
        kwargs: Optional[Dict[str, Any]] = None,
        timeout: Optional[float] = None,
        **method_kwargs: Any,
    ) -> Any:
        merged_kwargs = {**(kwargs or {}), **method_kwargs}
        return self._actor.call(
            self._method, *args, kwargs=merged_kwargs or None, timeout=timeout
        )

    def stream(
        self,
        *args: Any,
        kwargs: Optional[Dict[str, Any]] = None,
        timeout: Optional[float] = None,
        **method_kwargs: Any,
    ) -> Iterator[Any]:
        merged_kwargs = {**(kwargs or {}), **method_kwargs}
        return self._actor.stream(
            self._method, *args, kwargs=merged_kwargs or None, timeout=timeout
        )
