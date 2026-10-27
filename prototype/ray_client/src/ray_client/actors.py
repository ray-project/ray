"""ActorHandle and method proxies for persistent top-level Ray Actors."""

from typing import TYPE_CHECKING, Any, Dict, Optional

from ray_client.packaging import to_json

if TYPE_CHECKING:
    from ray_client.client import RayCluster


class ActorHandle:
    """Handle to a persistent top-level Ray Actor managed through `/api/actors`."""

    def __init__(
        self,
        name: str,
        cluster: "RayCluster",
        *,
        created: bool = False,
        package_uploaded: bool = False,
    ) -> None:
        self._name = name
        self._cluster = cluster
        self._created = created
        self._package_uploaded = package_uploaded

    def __repr__(self) -> str:
        return f"ActorHandle(name={self._name!r}, address={self._cluster.address!r})"

    @property
    def name(self) -> str:
        return self._name

    @property
    def created(self) -> bool:
        return self._created

    @property
    def package_uploaded(self) -> bool:
        return self._package_uploaded

    def info(self) -> Dict[str, Any]:
        return self._cluster._json("GET", f"/api/actors/{self._name}")

    def logs(self) -> str:
        """Returns captured stdout/stderr logs from the root actor process."""
        resp = self._cluster._json("GET", f"/api/actors/{self._name}/logs")
        if isinstance(resp, dict):
            return str(resp.get("logs", ""))
        return str(resp)

    def call(
        self,
        method: str,
        *args: Any,
        kwargs: Optional[Dict[str, Any]] = None,
        timeout: Optional[float] = None,
    ) -> Any:
        """Invokes `method(*args, **kwargs)` on the remote actor.

        Returns the JSON result for a regular/async method, or an SSE iterator of
        yielded JSON items if the method is a generator or async generator.
        """
        to_json(list(args), f"Actor '{self._name}.{method}' positional arguments")
        to_json(dict(kwargs or {}), f"Actor '{self._name}.{method}' keyword arguments")
        body: Dict[str, Any] = {
            "args": list(args),
            "kwargs": dict(kwargs or {}),
        }
        endpoint = f"/api/actors/{self._name}/methods/{method}"
        return self._cluster._call_or_stream_actor_method(
            endpoint,
            body,
            target=f"{self._name}.{method}",
            submission_id=f"{self._name}.{method}",
            timeout=timeout,
            is_actor_method=True,
        )

    def kill(self) -> bool:
        resp = self._cluster.request("DELETE", f"/api/actors/{self._name}")
        if resp.status_code == 404:
            return False
        if resp.status_code != 200:
            raise self._cluster._http_error("DELETE", f"/api/actors/{self._name}", resp)
        return bool(resp.json().get("killed", True))

    def __getattr__(self, method: str) -> "_ActorMethodProxy":
        if method.startswith("_"):
            raise AttributeError(method)
        return _ActorMethodProxy(self, method)


class _ActorMethodProxy:
    """Bound method proxy on `ActorHandle` supporting `actor.method(*args, **kwargs)` and `.call()`."""

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


ActorProxy = ActorHandle
