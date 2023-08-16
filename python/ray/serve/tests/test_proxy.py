import pytest
import ray
from ray import serve
from ray.actor import ActorHandle
from ray.serve._private.constants import (
    DEFAULT_UVICORN_TIMEOUT_KEEP_ALIVE_S,
    SERVE_NAMESPACE,
)


class TestTimeoutKeepAliveConfig:
    """Test setting timeout_keep_alive_s in config and env."""

    def get_proxy_actor(self) -> ActorHandle:
        proxy_actor_name = None
        for actor in ray._private.state.actors().values():
            if actor["ActorClassName"] == "HTTPProxyActor":
                proxy_actor_name = actor["Name"]
        return ray.get_actor(proxy_actor_name, namespace=SERVE_NAMESPACE)

    def test_default_timeout_keep_alive_s(self, ray_shutdown):
        """Test when no timeout_keep_alive_s is set.

        When the timeout_keep_alive_s is not set, the uvicorn keep alive is 600.
        """
        serve.start()
        proxy_actor = self.get_proxy_actor()
        assert (
            ray.get(proxy_actor._uvicorn_keep_alive.remote())
            == DEFAULT_UVICORN_TIMEOUT_KEEP_ALIVE_S
        )

    def test_set_timeout_keep_alive_in_http_configs(self, ray_shutdown):
        """Test when timeout_keep_alive_s is in http configs.

        When the timeout_keep_alive_s is set in http configs, the uvicorn keep alive
        is set correctly.
        """
        timeout_keep_alive_s = 222
        serve.start(http_options={"timeout_keep_alive_s": timeout_keep_alive_s})
        proxy_actor = self.get_proxy_actor()
        assert ray.get(proxy_actor._uvicorn_keep_alive.remote()) == timeout_keep_alive_s

    @pytest.mark.parametrize(
        "ray_instance",
        [
            {"RAY_SERVE_HTTP_KEEPALIVE_TIMEOUT_S": "333"},
        ],
        indirect=True,
    )
    def test_set_timeout_keep_alive_in_env(self, ray_instance, ray_shutdown):
        """Test when timeout_keep_alive_s is in env.

        When the timeout_keep_alive_s is set in env, the uvicorn keep alive
        is set correctly.
        """
        serve.start()
        proxy_actor = self.get_proxy_actor()
        assert ray.get(proxy_actor._uvicorn_keep_alive.remote()) == 333

    @pytest.mark.parametrize(
        "ray_instance",
        [
            {"RAY_SERVE_HTTP_KEEPALIVE_TIMEOUT_S": "333"},
        ],
        indirect=True,
    )
    def test_set_timeout_keep_alive_in_both_config_and_env(
        self, ray_instance, ray_shutdown
    ):
        """Test when timeout_keep_alive_s is in both http configs and env.

        When the timeout_keep_alive_s is set in env, the uvicorn keep alive
        is set to the one in env.
        """
        timeout_keep_alive_s = 222
        serve.start(http_options={"timeout_keep_alive_s": timeout_keep_alive_s})
        proxy_actor = self.get_proxy_actor()
        assert ray.get(proxy_actor._uvicorn_keep_alive.remote()) == 333


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
