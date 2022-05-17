import pytest
import os
import sys
import yaml
import ray
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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
