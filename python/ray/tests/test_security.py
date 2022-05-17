import pytest
import os
import sys
import yaml
import ray
import pathlib
import tempfile
import numpy as np
from ray._private.security import ENV_VAR_NAME

config_path = "/tmp/test.yaml"


@pytest.fixture
def enable_whitelist():
    os.environ[ENV_VAR_NAME] = config_path
    yield
    del os.environ[ENV_VAR_NAME]


@pytest.mark.parametrize("valid_whitelist", [True, False])
def test_remote_function_whitelist(shutdown_only, enable_whitelist, valid_whitelist):
    whitelist_config = {
        "remote_function_whitelist": [
            "test_security*" if valid_whitelist else "abc.def"
        ]
    }
    yaml.safe_dump(whitelist_config, open(config_path, "wt"))
    ray.init()

    @ray.remote
    class Foo:
        def foo(self):
            return "hello"

    @ray.remote
    def foo():
        return "world"

    # test for actor
    f = Foo.remote()
    if valid_whitelist:
        assert ray.get(f.foo.remote()) == "hello"
    else:
        with pytest.raises(ray.exceptions.RayActorError):
            ray.get(f.foo.remote())
    # test for normal task
    if valid_whitelist:
        assert ray.get(foo.remote()) == "world"
    else:
        with pytest.raises(ray.exceptions.WorkerCrashedError):
            ray.get(foo.remote())


def test_restricted_loads(shutdown_only):
    config_path = "/tmp/test.yaml"
    with tempfile.TemporaryDirectory() as tmp_dir:
        whitelist_config = {
            "pickle_whitelist": {
                "module": {
                    "numpy.core.numeric": ["*"],
                    "numpy": ["dtype"],
                },
                "package": ["pathlib"]
            }
        }
        yaml.safe_dump(whitelist_config, open(config_path, "wt"))
        ray.ray_constants.RAY_PICKLE_WHITELIST_CONFIG_PATH = config_path
        ray._private.security.patch_pickle_for_security()
        ray.init()
        data = np.zeros((10, 10))
        ref1 = ray.put(data)
        ray.get(ref1)

        ref2 = ray.put([ref1])
        ray.get(ref2)

        ref3 = ray.put([pathlib.Path("/tmp/test")])
        ray.get(ref3)

        class WrongClass:
            pass
        ref4 = ray.put(WrongClass())
        with pytest.raises(ray.exceptions.RaySystemError) as error:
            ray.get(ref4)
        assert isinstance(error.value.args[0], ray.cloudpickle.UnpicklingError)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
