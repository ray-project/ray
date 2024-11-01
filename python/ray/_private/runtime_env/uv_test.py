from ray._private.runtime_env import uv

import unittest
from unittest.mock import AsyncMock


class TestRuntimeEnv:
    def uv_config(self):
        return {"packages": ["requests"]}

    def env_vars(self):
        return {}


class UvProcessorTest(unittest.IsolatedAsyncioTestCase):
    async def test_run(self):
        target_dir = "/tmp"
        runtime_env = TestRuntimeEnv()

        aync_mock = AsyncMock(return_value=None)
        uv_processor = uv.UvProcessor(target_dir=target_dir, runtime_env=runtime_env)

        with unittest.mock.patch("uv.UvProcessor._install_uv", aync_mock):
            with unittest.mock.patch("uv.UvProcessor._install_uv_packages", aync_mock):
                await uv_processor._run()


if __name__ == "__main__":
    unittest.main()
