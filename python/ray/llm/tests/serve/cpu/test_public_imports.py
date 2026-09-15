import sys

import pytest


def test_sglang_server_public_export():
    from ray.llm._internal.serve.engines.sglang import (
        SGLangServer as InternalSGLangServer,
    )
    from ray.serve.llm import SGLangServer
    from ray.serve.llm.deployment import SGLangServer as DeploymentSGLangServer

    assert SGLangServer is DeploymentSGLangServer
    assert issubclass(SGLangServer, InternalSGLangServer)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
