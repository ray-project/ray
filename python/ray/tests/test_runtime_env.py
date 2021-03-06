import pytest
from ray.test_utils import run_string_as_driver

driver_script = """
import sys
sys.path.insert(0, "{working_dir}")
import test_module
import ray

job_config = ray.job_config.JobConfig(
    runtime_env={runtime_env}
)

ray.init(address="{redis_address}", job_config=job_config)

@ray.remote
def run_test():
    return test_module.one()

print(sum(ray.get([run_test.remote()] * 1000)))

ray.shutdown()"""


@pytest.fixture
def working_dir():
    import tempfile
    from pathlib import Path
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
        yield tmp_dir


def test_single_node(ray_start_cluster_head, working_dir):
    cluster = ray_start_cluster_head
    redis_address = cluster.address
    runtime_env = f"""{{  "working_dir": "{working_dir}" }}"""
    script = driver_script.format(
        redis_address=redis_address,
        working_dir=working_dir,
        runtime_env=runtime_env)

    out = run_string_as_driver(script)
    assert out.strip().split()[-1] == "1000"


def test_two_node(two_node_cluster, working_dir):
    cluster, _ = two_node_cluster
    redis_address = cluster.address
    runtime_env = f"""{{  "working_dir": "{working_dir}" }}"""
    script = driver_script.format(
        redis_address=redis_address,
        working_dir=working_dir,
        runtime_env=runtime_env)
    out = run_string_as_driver(script)
    assert out.strip().split()[-1] == "1000"


def test_two_node_module(two_node_cluster, working_dir):
    cluster, _ = two_node_cluster
    redis_address = cluster.address
    runtime_env = """{  "local_modules": [test_module] }"""
    script = driver_script.format(
        redis_address=redis_address,
        working_dir=working_dir,
        runtime_env=runtime_env)
    print(script)
    out = run_string_as_driver(script)
    assert out.strip().split()[-1] == "1000"


def test_two_node_uri(two_node_cluster, working_dir):
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
    script = driver_script.format(
        redis_address=redis_address,
        working_dir=working_dir,
        runtime_env=runtime_env)
    out = run_string_as_driver(script)
    assert out.strip().split()[-1] == "1000"


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-sv", __file__]))
