from typing import TYPE_CHECKING, Optional

import ray
from ray.serve._private.constants import SERVE_CONTROLLER_NAME, SERVE_NAMESPACE
from ray.serve._private.default_impl import get_controller_impl
from ray.serve.config import HTTPOptions, ProxyLocation
from ray.serve.schema import LoggingConfig

if TYPE_CHECKING:
    from ray.actor import ActorHandle


@ray.remote(num_cpus=0)
class ServeControllerAvatar:
    """A hack that proxy the creation of async actors from Java.

    To be removed after https://github.com/ray-project/ray/pull/26037

    Java api can not support python async actor. If we use java api create
    python async actor. The async init method won't be executed. The async
    method will fail with pickle error. And the run_control_loop of controller
    actor can't be executed too. We use this proxy actor create python async
    actor to avoid the above problem.
    """

    def __init__(
        self,
        http_proxy_port: int = 8000,
    ):
        try:
            self._controller: Optional["ActorHandle"] = ray.get_actor(
                SERVE_CONTROLLER_NAME, namespace=SERVE_NAMESPACE
            )
        except ValueError:
            self._controller = None
        if self._controller is None:
            controller_impl = get_controller_impl()
            # This Java bootstrap builds HTTPOptions directly and previously
            # relied on the (now removed) HeadOnly default of
            # HTTPOptions.location. Pass proxy_location explicitly to preserve
            # head-only proxy placement on multi-node clusters.
            self._controller = controller_impl.remote(
                http_options=HTTPOptions(port=http_proxy_port),
                proxy_location=ProxyLocation.HeadOnly,
                global_logging_config=LoggingConfig(),
            )

    def check_alive(self) -> None:
        """No-op to check if this actor is alive."""
        return
