import pytest
import sys
import unittest
from pathlib import Path
from ray.test_utils import run_string_as_driver
import ray
driver_script = """
import sys
import logging
sys.path.insert(0, "{working_dir}")
import test_module
import ray

job_config = ray.job_config.JobConfig(
    runtime_env={runtime_env}
)

ray.init(address="{redis_address}",
         job_config=job_config,
         logging_level=logging.DEBUG)

@ray.remote
def run_test():
    return test_module.one()

@ray.remote
class TestActor(object):
    @ray.method(num_returns=1)
    def one(self):
        return test_module.one()


{execute_statement}

ray.shutdown()"""


@pytest.fixture
def working_env():
    import tempfile
    from ray._private.runtime_env import (
        get_project_package_name,
        _get_local_path
    )
    with tempfile.TemporaryDirectory() as tmp_dir:
        path = Path(tmp_dir)
        module_path = path / "test_module"
        module_path.mkdir(parents=True)
        init_file = module_path / "__init__.py"
        test_file = module_path / "test.py"
        with test_file.open(mode="w") as f:
            f.write("""
def one():
    return 1
""")
        with init_file.open(mode="w") as f:
            f.write("""
from test_module.test import one
""")

        yield (tmp_dir, get_project_package_name(tmp_dir, []))


@unittest.skipIf(sys.platform == "win32", "Fail to create temp dir.")
def test_single_node(ray_start_cluster_head, working_env):
    (working_dir, pkg_name) = working_env
    cluster = ray_start_cluster_head
    redis_address = cluster.address
    runtime_env = f"""{{  "working_dir": "{working_dir}" }}"""
    execute_statement = "print(sum(ray.get([run_test.remote()] * 1000)))"
    script = driver_script.format(**locals())
    out = run_string_as_driver(script)
    assert out.strip().split()[-1] == "1000"
    from ray._private.runtime_env import PKG_DIR
    pkg_path = Path(PKG_DIR) / pkg_name
    assert not pkg_path.exists()


@unittest.skipIf(sys.platform == "win32", "Fail to create temp dir.")
def test_two_node(two_node_cluster, working_env):
    (working_dir, pkg_name) = working_env
    cluster, _ = two_node_cluster
    redis_address = cluster.address
    runtime_env = f"""{{  "working_dir": "{working_dir}" }}"""
    execute_statement = "print(sum(ray.get([run_test.remote()] * 1000)))"
    script = driver_script.format(**locals())
    out = run_string_as_driver(script)
    assert out.strip().split()[-1] == "1000"
    from ray._private.runtime_env import PKG_DIR
    pkg_path = Path(PKG_DIR) / pkg_name
    assert not pkg_path.exists()


@unittest.skipIf(sys.platform == "win32", "Fail to create temp dir.")
def test_two_node_module(two_node_cluster, working_env):
    (working_dir, pkg_name) = working_env
    cluster, _ = two_node_cluster
    redis_address = cluster.address
    runtime_env = """{  "local_modules": [test_module] }"""
    execute_statement = "print(sum(ray.get([run_test.remote()] * 1000)))"
    script = driver_script.format(**locals())
    print(script)
    out = run_string_as_driver(script)
    assert out.strip().split()[-1] == "1000"
    from ray._private.runtime_env import PKG_DIR
    pkg_path = Path(PKG_DIR) / pkg_name
    assert not pkg_path.exists()


@unittest.skipIf(sys.platform == "win32", "Fail to create temp dir.")
def test_two_node_uri(two_node_cluster, working_env):
    (working_dir, pkg_name) = working_env
    cluster, _ = two_node_cluster
    redis_address = cluster.address
    import ray._private.runtime_env as runtime_env
    import tempfile
    with tempfile.NamedTemporaryFile(suffix="zip") as tmp_file:
        pkg_name = runtime_env.get_project_package_name(working_dir, [])
        pkg_uri = runtime_env.Protocol.PIN_GCS.value + "://" + pkg_name
        runtime_env.create_project_package(working_dir, [], tmp_file.name)
        runtime_env.push_package(pkg_uri, tmp_file.name)
        runtime_env = f"""{{ "working_dir_uri": "{pkg_uri}" }}"""
        execute_statement = "print(sum(ray.get([run_test.remote()] * 1000)))"
    script = driver_script.format(**locals())
    out = run_string_as_driver(script)
    assert out.strip().split()[-1] == "1000"
    from ray._private.runtime_env import PKG_DIR
    pkg_path = Path(PKG_DIR) / pkg_name
    assert not pkg_path.exists()


@unittest.skipIf(sys.platform == "win32", "Fail to create temp dir.")
def test_regular_actors(ray_start_cluster_head, working_env):
    (working_dir, pkg_name) = working_env
    cluster = ray_start_cluster_head
    redis_address = cluster.address
    runtime_env = f"""{{  "working_dir": "{working_dir}" }}"""
    execute_statement = """
test_actor = TestActor.options(name="test_actor").remote()
print(sum(ray.get([test_actor.one.remote()] * 1000)))
"""
    script = driver_script.format(**locals())
    out = run_string_as_driver(script)
    assert out.strip().split()[-1] == "1000"
    from ray._private.runtime_env import PKG_DIR
    pkg_path = Path(PKG_DIR) / pkg_name
    assert not pkg_path.exists()


@unittest.skipIf(sys.platform == "win32", "Fail to create temp dir.")
def test_detached_actors(ray_start_cluster_head, working_env):
    (working_dir, pkg_name) = working_env
    cluster = ray_start_cluster_head
    redis_address = cluster.address
    runtime_env = f"""{{  "working_dir": "{working_dir}" }}"""
    execute_statement = """
test_actor = TestActor.options(name="test_actor", lifetime="detached").remote()
print(sum(ray.get([test_actor.one.remote()] * 1000)))
"""
    script = driver_script.format(**locals())
    out = run_string_as_driver(script)
    assert out.strip().split()[-1] == "1000"
    from ray._private.runtime_env import PKG_DIR
    pkg_path = Path(PKG_DIR) / pkg_name
    # It's a detached actors, so it should still be there
    assert pkg_path.exists()
    test_actor = ray.get_actor("test_actor")
    assert sum(ray.get([test_actor.one.remote()] * 1000)) == 1000
    ray.kill(test_actor)
    assert not pkg_path.exists()



if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-sv", __file__]))
