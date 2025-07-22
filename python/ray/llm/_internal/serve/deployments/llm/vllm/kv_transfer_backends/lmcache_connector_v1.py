from ray.llm._internal.serve.deployments.llm.vllm.kv_transfer_backends.base import (
    BaseConnectorBackend,
)


def _check_lmcache_installed():
    try:
        import lmcache  # noqa: F401
    except ImportError:
        raise ImportError(
            "LMCache is not installed. Please install it with `pip install lmcache`."
        )


class LMCacheConnectorV1Backend(BaseConnectorBackend):
    def setup(self) -> None:
        """
        No manipulations with LMCacheConnectorV1 is needed.
        vLLM will configure the LMCache connector.
        """
        _check_lmcache_installed()
