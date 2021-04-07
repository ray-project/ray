import unittest
from unittest.mock import patch

from ray.rllib.env.wrappers.unity3d_env import Unity3DEnv


@patch("mlagents_envs.environment.UnityEnvironment")
class TestUnity3DEnv(unittest.TestCase):
    def test_port_editor(self, mock_unity3d):
        """Test if the environment uses the editor port
         when no environment file is provided"""

        _ = Unity3DEnv(port=None)
        args, kwargs = mock_unity3d.call_args
        mock_unity3d.assert_called_once()
        self.assertEqual(5004, kwargs.get("base_port"))

    def test_port_app(self, mock_unity3d):
        """Test if the environment uses the correct port
        when the environment file is provided"""

        _ = Unity3DEnv(file_name="app", port=None)
        args, kwargs = mock_unity3d.call_args
        mock_unity3d.assert_called_once()
        self.assertEqual(5005, kwargs.get("base_port"))

    def test_ports_multi_app(self, mock_unity3d):
        """Test if the base_port + worker_id
        is different for each environment"""

        _ = Unity3DEnv(file_name="app", port=None)
        args, kwargs_first = mock_unity3d.call_args
        _ = Unity3DEnv(file_name="app", port=None)
        args, kwargs_second = mock_unity3d.call_args
        self.assertNotEqual(
            kwargs_first.get("base_port") + kwargs_first.get("worker_id"),
            kwargs_second.get("base_port") + kwargs_second.get("worker_id"))

    def test_custom_port_app(self, mock_unity3d):
        """Test if the base_port + worker_id is different
        for each environment when using custom ports"""

        _ = Unity3DEnv(file_name="app", port=5010)
        args, kwargs_first = mock_unity3d.call_args
        _ = Unity3DEnv(file_name="app", port=5010)
        args, kwargs_second = mock_unity3d.call_args
        self.assertNotEqual(
            kwargs_first.get("base_port") + kwargs_first.get("worker_id"),
            kwargs_second.get("base_port") + kwargs_second.get("worker_id"))


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
