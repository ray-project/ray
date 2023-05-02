import asyncio
import concurrent.futures
from dataclasses import dataclass
from functools import wraps
import inspect
import os
from typing import Coroutine, Dict, Optional, Union
import threading

import ray
from ray._private.utils import get_or_create_event_loop
from ray.actor import ActorHandle

from ray import serve
from ray.serve._private.common import EndpointTag
from ray.serve._private.constants import (
    SERVE_HANDLE_JSON_KEY,
    SYNC_HANDLE_IN_DAG_FEATURE_FLAG_ENV_KEY,
    ServeHandleType,
)
from ray.serve._private.utils import (
    get_random_letters,
    DEFAULT,
)
from ray.serve._private.autoscaling_metrics import start_metrics_pusher
from ray.serve._private.common import DeploymentInfo
from ray.serve._private.constants import HANDLE_METRIC_PUSH_INTERVAL_S
from ray.serve.generated.serve_pb2 import DeploymentRoute
from ray.serve._private.router import Router, RequestMetadata
from ray.util import metrics
from ray.util.annotations import DeveloperAPI, PublicAPI

_global_async_loop = None


# Feature flag to revert to legacy behavior of synchronous deployment
# handle in dynamic dispatch. This is here as an escape hatch and last resort.
FLAG_SERVE_DEPLOYMENT_HANDLE_IS_SYNC = (
    os.environ.get(SYNC_HANDLE_IN_DAG_FEATURE_FLAG_ENV_KEY, "0") == "1"
)


def _wrap_into_async_task(async_func):
    """Wrap an async function so it returns async task instead of coroutine

    This makes the returned value awaitable more than once.
    """
    assert inspect.iscoroutinefunction(async_func)

    @wraps(async_func)
    def wrapper(*args, **kwargs):
        return asyncio.ensure_future(async_func(*args, **kwargs))

    return wrapper


def _create_or_get_async_loop_in_thread():
    global _global_async_loop
    if _global_async_loop is None:
        _global_async_loop = asyncio.new_event_loop()
        thread = threading.Thread(
            daemon=True,
            target=_global_async_loop.run_forever,
        )
        thread.start()
    return _global_async_loop


@PublicAPI(stability="beta")
@dataclass(frozen=True)
class HandleOptions:
    """Options for each ServeHandle instances. These fields are immutable."""

    method_name: str = "__call__"


@PublicAPI(stability="beta")
class RayServeHandle:
    """A handle used to make requests from one deployment to another.

    This is used to compose multiple deployments in a single application by binding
    them together when building the application. For example:

    .. code-block:: python

        import ray
        from ray import serve
        from ray.serve.handle import RayServeHandle, RayServeSyncHandle

        @serve.deployment
        class Downstream:
            def __init__(self, message: str):
                self._message = message

        def __call__(self, name: str) -> str:
            return self._message + name

        @serve.deployment
        class Ingress:
            def __init__(self, handle: RayServeHandle):
                self._handle = handle

            async def __call__(self, name: str) -> str:
                obj_ref: ray.ObjectRef = await self._handle.remote(name)
                return await obj_ref

        app = Ingress.bind(Downstream.bind("Hello "))
        handle: RayServeSyncHandle = serve.run(app)

        # Prints "Hello Mr. Magoo"
        print(ray.get(handle.remote("Mr. Magoo")))

    """

    def __init__(
        self,
        controller_handle: ActorHandle,
        deployment_name: EndpointTag,
        handle_options: Optional[HandleOptions] = None,
        *,
        _router: Optional[Router] = None,
        _internal_pickled_http_request: bool = False,
    ):
        self.controller_handle = controller_handle
        self.deployment_name = deployment_name
        self.handle_options = handle_options or HandleOptions()
        self.handle_tag = f"{self.deployment_name}#{get_random_letters()}"
        self._pickled_http_request = _internal_pickled_http_request

        self.request_counter = metrics.Counter(
            "serve_handle_request_counter",
            description=(
                "The number of handle.remote() calls that have been "
                "made on this handle."
            ),
            tag_keys=("handle", "deployment", "route", "application"),
        )
        self.request_counter.set_default_tags(
            {"handle": self.handle_tag, "deployment": self.deployment_name}
        )

        self.router: Router = _router or self._make_router()

        deployment_route = DeploymentRoute.FromString(
            ray.get(
                self.controller_handle.get_deployment_info.remote(self.deployment_name)
            )
        )
        deployment_info = DeploymentInfo.from_proto(deployment_route.deployment_info)

        self._stop_event: Optional[threading.Event] = None
        self._pusher: Optional[threading.Thread] = None
        remote_func = self.controller_handle.record_handle_metrics.remote
        if deployment_info.deployment_config.autoscaling_config:
            self._stop_event = threading.Event()
            self._pusher = start_metrics_pusher(
                interval_s=HANDLE_METRIC_PUSH_INTERVAL_S,
                collection_callback=self._collect_handle_queue_metrics,
                metrics_process_func=remote_func,
                stop_event=self._stop_event,
            )

    def _collect_handle_queue_metrics(self) -> Dict[str, int]:
        return {self.deployment_name: self.router.get_num_queued_queries()}

    def _make_router(self) -> Router:
        return Router(
            self.controller_handle,
            self.deployment_name,
            event_loop=get_or_create_event_loop(),
        )

    def stop_metrics_pusher(self):
        if self._stop_event and self._pusher:
            self._stop_event.set()
            self._pusher.join()

    @property
    def _is_polling(self) -> bool:
        """Whether this handle is actively polling for replica updates."""
        return self.router.long_poll_client.is_running

    @property
    def _is_same_loop(self) -> bool:
        """Whether the caller's asyncio loop is the same loop for handle.

        This is only useful for async handles.
        """
        return get_or_create_event_loop() == self.router._event_loop

    def _options(
        self,
        *,
        method_name: Union[str, DEFAULT] = DEFAULT.VALUE,
    ):
        new_options_dict = self.handle_options.__dict__.copy()
        user_modified_options_dict = {
            key: value
            for key, value in zip(["method_name"], [method_name])
            if value != DEFAULT.VALUE
        }
        new_options_dict.update(user_modified_options_dict)
        new_options = HandleOptions(**new_options_dict)

        return self.__class__(
            self.controller_handle,
            self.deployment_name,
            new_options,
            _router=self.router,
            _internal_pickled_http_request=self._pickled_http_request,
        )

    def options(
        self,
        *,
        method_name: Union[str, DEFAULT] = DEFAULT.VALUE,
    ) -> "RayServeHandle":
        """Set options for this handle and return an updated copy of it.

        Example:

        .. code-block:: python

            # The following two lines are equivalent:
            obj_ref = await handle.other_method.remote(*args)
            obj_ref = await handle.options(method_name="other_method").remote(*args)

        """
        return self._options(method_name=method_name)

    def _remote(self, deployment_name, handle_options, args, kwargs) -> Coroutine:
        _request_context = ray.serve.context._serve_request_context.get()
        request_metadata = RequestMetadata(
            _request_context.request_id,
            deployment_name,
            call_method=handle_options.method_name,
            http_arg_is_pickled=self._pickled_http_request,
            route=_request_context.route,
            app_name=_request_context.app_name,
        )
        self.request_counter.inc(
            tags={
                "route": _request_context.route,
                "application": _request_context.app_name,
            }
        )
        coro = self.router.assign_request(request_metadata, *args, **kwargs)
        return coro

    @_wrap_into_async_task
    async def remote(self, *args, **kwargs) -> asyncio.Task:
        """Issue an asynchronous request to the __call__ method of the deployment.

        Returns an `asyncio.Task` whose underlying result is a Ray ObjectRef that
        points to the final result of the request.

        The final result can be retrieved by `await`ing the ObjectRef. Example:

        .. code-block:: python

            obj_ref = await handle.remote(*args)
            result = await obj_ref

        """
        return await self._remote(
            self.deployment_name, self.handle_options, args, kwargs
        )

    def __repr__(self):
        return f"{self.__class__.__name__}" f"(deployment='{self.deployment_name}')"

    @classmethod
    def _deserialize(cls, kwargs):
        """Required for this class's __reduce__ method to be picklable."""
        return cls(**kwargs)

    def __reduce__(self):
        serialized_data = {
            "controller_handle": self.controller_handle,
            "deployment_name": self.deployment_name,
            "handle_options": self.handle_options,
            "_internal_pickled_http_request": self._pickled_http_request,
        }
        return RayServeHandle._deserialize, (serialized_data,)

    def __getattr__(self, name):
        return self.options(method_name=name)

    def __del__(self):
        self.stop_metrics_pusher()


@PublicAPI(stability="beta")
class RayServeSyncHandle(RayServeHandle):
    """A handle used to make requests to the ingress deployment of an application.

    This is returned by `serve.run` and can be used to invoke the application from
    Python rather than over HTTP. For example:

    .. code-block:: python

        import ray
        from ray import serve
        from ray.serve.handle import RayServeSyncHandle

        @serve.deployment
        class Ingress:
            def __call__(self, name: str) -> str:
                return f"Hello {name}"

        app = Ingress.bind()
        handle: RayServeSyncHandle = serve.run(app)

        # Prints "Hello Mr. Magoo"
        print(ray.get(handle.remote("Mr. Magoo")))

    """

    @property
    def _is_same_loop(self) -> bool:
        # NOTE(simon): For sync handle, the caller doesn't have to be in the
        # same loop as the handle's loop, so we always return True here.
        return True

    def _make_router(self) -> Router:
        # Delayed import because ray.serve.api depends on handles.
        return Router(
            self.controller_handle,
            self.deployment_name,
            event_loop=_create_or_get_async_loop_in_thread(),
        )

    def options(
        self,
        *,
        method_name: Union[str, DEFAULT] = DEFAULT.VALUE,
    ) -> "RayServeSyncHandle":
        """Set options for this handle and return an updated copy of it.

        Example:

        .. code-block:: python

            # The following two lines are equivalent:
            obj_ref = handle.other_method.remote(*args)
            obj_ref = handle.options(method_name="other_method").remote(*args)

        """
        return self._options(method_name=method_name)

    def remote(self, *args, **kwargs) -> ray.ObjectRef:
        """Issue an asynchronous request to the __call__ method of the deployment.

        Returns a Ray ObjectRef whose results can be waited for or retrieved
        using ray.wait or ray.get, respectively.

        .. code-block:: python

            obj_ref = handle.remote(*args)
            result = ray.get(obj_ref)

        """
        coro = self._remote(self.deployment_name, self.handle_options, args, kwargs)
        future: concurrent.futures.Future = asyncio.run_coroutine_threadsafe(
            coro, self.router._event_loop
        )
        return future.result()

    def __reduce__(self):
        serialized_data = {
            "controller_handle": self.controller_handle,
            "deployment_name": self.deployment_name,
            "handle_options": self.handle_options,
            "_internal_pickled_http_request": self._pickled_http_request,
        }
        return RayServeSyncHandle._deserialize, (serialized_data,)


@DeveloperAPI
class RayServeDeploymentHandle:
    """Send requests to a deployment. This class should not be manually created."""

    # """Lazily initialized handle that only gets fulfilled upon first execution."""
    def __init__(
        self,
        deployment_name: str,
        handle_options: Optional[HandleOptions] = None,
    ):
        self.deployment_name = deployment_name
        self.handle_options = handle_options or HandleOptions()
        # For Serve DAG we need placeholder in DAG binding and building without
        # requirement of serve.start; Thus handle is fulfilled at runtime.
        self.handle: RayServeHandle = None

    def options(self, *, method_name: str) -> "RayServeDeploymentHandle":
        return self.__class__(
            self.deployment_name, HandleOptions(method_name=method_name)
        )

    def remote(self, *args, _ray_cache_refs: bool = False, **kwargs) -> asyncio.Task:
        if not self.handle:
            handle = serve._private.api.get_deployment(
                self.deployment_name
            )._get_handle(sync=FLAG_SERVE_DEPLOYMENT_HANDLE_IS_SYNC)
            self.handle = handle.options(method_name=self.handle_options.method_name)
        return self.handle.remote(*args, **kwargs)

    @classmethod
    def _deserialize(cls, kwargs):
        """Required for this class's __reduce__ method to be picklable."""
        return cls(**kwargs)

    def __reduce__(self):
        serialized_data = {
            "deployment_name": self.deployment_name,
            "handle_options": self.handle_options,
        }
        return RayServeDeploymentHandle._deserialize, (serialized_data,)

    def __getattr__(self, name):
        return self.options(method_name=name)

    def __repr__(self):
        return f"{self.__class__.__name__}" f"(deployment='{self.deployment_name}')"


def _serve_handle_to_json_dict(handle: RayServeHandle) -> Dict[str, str]:
    """Converts a Serve handle to a JSON-serializable dictionary.

    The dictionary can be converted back to a ServeHandle using
    _serve_handle_from_json_dict.
    """
    if isinstance(handle, RayServeSyncHandle):
        handle_type = ServeHandleType.SYNC
    else:
        handle_type = ServeHandleType.ASYNC

    return {
        SERVE_HANDLE_JSON_KEY: handle_type,
        "deployment_name": handle.deployment_name,
    }


def _serve_handle_from_json_dict(d: Dict[str, str]) -> RayServeHandle:
    """Converts a JSON-serializable dictionary back to a ServeHandle.

    The dictionary should be constructed using _serve_handle_to_json_dict.
    """
    if SERVE_HANDLE_JSON_KEY not in d:
        raise ValueError(f"dict must contain {SERVE_HANDLE_JSON_KEY} key.")

    return serve.context.get_global_client().get_handle(
        d["deployment_name"],
        sync=d[SERVE_HANDLE_JSON_KEY] == ServeHandleType.SYNC,
        missing_ok=True,
    )
