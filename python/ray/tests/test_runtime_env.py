import os
import pytest
import sys
import tempfile
import time
import requests
from pathlib import Path
from pytest_lazyfixture import lazy_fixture

import ray
from ray.exceptions import RuntimeEnvSetupError
import ray.experimental.internal_kv as kv
from ray._private.test_utils import (run_string_as_driver, wait_for_condition)
from ray._private.runtime_env.packaging import GCS_STORAGE_MAX_SIZE, parse_uri
from ray._private.utils import (get_wheel_filename, get_master_wheel_url,
                                get_release_wheel_url)

S3_PACKAGE_URI = "s3://runtime-env-test/remote_runtime_env.zip"

driver_script = """
import logging
import os
import sys
import time
import traceback

import ray
import ray.util

# Define test_module for py_module tests
try:
    import test_module
except:
    pass

try:
    job_config = ray.job_config.JobConfig(
        runtime_env={runtime_env}
    )

    if not job_config.runtime_env:
        job_config=None


    if os.environ.get("USE_RAY_CLIENT"):
        ray.client("{address}").env({runtime_env}).namespace("default_test_namespace").connect()
    else:
        ray.init(address="{address}",
                 job_config=job_config,
                 logging_level=logging.DEBUG,
                 namespace="default_test_namespace"
)
except ValueError:
    print("ValueError:", traceback.format_exc())
    sys.exit(0)
except TypeError:
    print("TypeError:", traceback.format_exc())
    sys.exit(0)
except Exception:
    print("ERROR:", traceback.format_exc())
    sys.exit(0)


if os.environ.get("EXIT_AFTER_INIT"):
    sys.exit(0)

# Schedule a dummy task to kick off runtime env agent's working_dir setup()
@ray.remote
def dummy_task():
    return "dummy task scheduled"

ray.get([dummy_task.remote()])

# Insert working_dir path with unzipped files
sys.path.insert(0, "{working_dir}")

# Actual import of test_module after working_dir is setup
import test_module

@ray.remote
def run_test():
    return test_module.one()

@ray.remote
def check_file(name):
    try:
        with open(name) as f:
            return f.read()
    except:
        return "FAILED"

@ray.remote
class TestActor(object):
    @ray.method(num_returns=1)
    def one(self):
        return test_module.one()

{execute_statement}

if os.environ.get("USE_RAY_CLIENT"):
    ray.util.disconnect()
else:
    ray.shutdown()
"""


def create_file(p):
    if not p.parent.exists():
        p.parent.mkdir()
    with p.open("w") as f:
        f.write("Test")


@pytest.fixture(scope="function")
def working_dir():
    """Regular local_working_dir test setup for existing tests"""
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

        old_dir = os.getcwd()
        os.chdir(tmp_dir)
        yield tmp_dir
        os.chdir(old_dir)


@pytest.fixture(scope="function")
def local_working_dir():
    """Parametrized local_working_dir test setup"""
    with tempfile.TemporaryDirectory() as tmp_dir:
        path = Path(tmp_dir)
        module_path = path / "test_module"
        module_path.mkdir(parents=True)

        # There are "test.py" file with same module name and function
        # signature, but different return value. Regular runtime env
        # working_dir uses existing file and should return 1 on each
        # call to one(); While s3 remote runtime env with same file
        # names will return 2.

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

        old_dir = os.getcwd()
        os.chdir(tmp_dir)
        runtime_env = f"""{{  "working_dir": "{tmp_dir}" }}"""
        # local working_dir's one() return 1 for each call
        yield tmp_dir, runtime_env, "1000"
        os.chdir(old_dir)


@pytest.fixture(scope="function")
def s3_working_dir():
    """Parametrized s3_working_dir test setup"""
    with tempfile.TemporaryDirectory() as tmp_dir:
        old_dir = os.getcwd()
        os.chdir(tmp_dir)

        # There are "test.py" file with same module name and function
        # signature, but different return value. Regular runtime env
        # working_dir uses existing file and should return 1 on each
        # call to one(); While s3 remote runtime env with same file
        # names will return 2.

        runtime_env = f"""{{  "working_dir": "{S3_PACKAGE_URI}" }}"""
        _, pkg_name = parse_uri(S3_PACKAGE_URI)
        runtime_env_dir = ray.worker._global_node.get_runtime_env_dir_path()
        working_dir = Path(os.path.join(runtime_env_dir,
                                        pkg_name)).with_suffix("")
        # s3 working_dir's one() return 2 for each call
        yield working_dir, runtime_env, "2000"
        os.chdir(old_dir)


@pytest.fixture(
    scope="function",
    params=[lazy_fixture("local_working_dir"),
            lazy_fixture("s3_working_dir")])
def working_dir_parametrized(request):
    return request.param


def start_client_server(cluster, client_mode):
    env = {}
    if client_mode:
        ray.worker._global_node._ray_params.ray_client_server_port = "10003"
        ray.worker._global_node.start_ray_client_server()
        address = "localhost:10003"
        env["USE_RAY_CLIENT"] = "1"
    else:
        address = cluster.address

    runtime_env_dir = ray.worker._global_node.get_runtime_env_dir_path()

    return address, env, runtime_env_dir


"""
The following test cases are related with runtime env. It following these steps
  1) Creating a temporary dir with fixture working_dir
  2) Using a template named driver_script defined globally
  3) Overwrite runtime_env and execute_statement in the template
  4) Execute it as a separate driver and return the result
"""


@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
@pytest.mark.parametrize("client_mode", [True, False])
def test_empty_working_dir(ray_start_cluster_head, client_mode):
    cluster = ray_start_cluster_head
    address, env, runtime_env_dir = start_client_server(cluster, client_mode)
    env["EXIT_AFTER_INIT"] = "1"
    with tempfile.TemporaryDirectory() as working_dir:
        runtime_env = f"""{{
    "working_dir": r"{working_dir}"
}}"""
        # Execute the following cmd in driver with runtime_env
        execute_statement = "sys.exit(0)"
        script = driver_script.format(**locals())
        out = run_string_as_driver(script, env)
        assert not out.startswith("ERROR:")


@pytest.mark.skip("py_modules not supported yet.")
@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
@pytest.mark.parametrize("client_mode", [True, False])
def test_empty_py_modules(ray_start_cluster_head, client_mode):
    cluster = ray_start_cluster_head
    address, env, runtime_env_dir = start_client_server(cluster, client_mode)
    env["EXIT_AFTER_INIT"] = "1"
    with tempfile.TemporaryDirectory() as working_dir:
        runtime_env = f"""{{
    "py_modules": [r"{working_dir}"]
}}"""
        # Execute the following cmd in driver with runtime_env
        execute_statement = "sys.exit(0)"
        script = driver_script.format(**locals())
        out = run_string_as_driver(script, env)
        assert not out.startswith("ERROR:")


@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
@pytest.mark.parametrize("client_mode", [True, False])
def test_invalid_working_dir(ray_start_cluster_head, working_dir, client_mode):
    cluster = ray_start_cluster_head
    address, env, runtime_env_dir = start_client_server(cluster, client_mode)
    env["EXIT_AFTER_INIT"] = "1"

    runtime_env = "{ 'working_dir': 10 }"
    # Execute the following cmd in driver with runtime_env
    execute_statement = ""
    script = driver_script.format(**locals())
    out = run_string_as_driver(script, env).strip().split()[-1]
    assert out.strip().splitlines()[-1].startswith("TypeError")

    runtime_env = f"{{ 'working_dir': os.path.join(r'{working_dir}', 'na') }}"
    # Execute the following cmd in driver with runtime_env
    execute_statement = ""
    script = driver_script.format(**locals())
    out = run_string_as_driver(script, env).strip().split()[-1]
    assert out.strip().splitlines()[-1].startswith("ValueError")

    runtime_env = "{ 'working_dir': 's3://bucket/package' }"
    # Execute the following cmd in driver with runtime_env
    execute_statement = ""
    script = driver_script.format(**locals())
    out = run_string_as_driver(script, env)
    assert out.strip().splitlines()[-1].startswith("ValueError")


@pytest.mark.skip("py_modules not supported yet.")
@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
@pytest.mark.parametrize("client_mode", [True, False])
def test_invalid_py_modules(ray_start_cluster_head, working_dir, client_mode):
    cluster = ray_start_cluster_head
    address, env, runtime_env_dir = start_client_server(cluster, client_mode)
    env["EXIT_AFTER_INIT"] = "1"

    runtime_env = "{ 'py_modules': [10] }"
    # Execute the following cmd in driver with runtime_env
    execute_statement = ""
    script = driver_script.format(**locals())
    out = run_string_as_driver(script, env).strip().split()[-1]
    assert out.strip().splitlines()[-1].startswith("TypeError")

    runtime_env = f"{{ 'py_modules': [os.path.join(r'{working_dir}', 'na')] }}"
    # Execute the following cmd in driver with runtime_env
    execute_statement = ""
    script = driver_script.format(**locals())
    out = run_string_as_driver(script, env).strip().split()[-1]
    assert out.strip().splitlines()[-1].startswith("ValueError")

    runtime_env = "{ 'py_modules': ['s3://bucket/package'] }"
    # Execute the following cmd in driver with runtime_env
    execute_statement = ""
    script = driver_script.format(**locals())
    out = run_string_as_driver(script, env)
    assert out.strip().splitlines()[-1].startswith("ValueError")


@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
@pytest.mark.parametrize("client_mode", [True, False])
def test_single_node(ray_start_cluster_head, working_dir_parametrized,
                     client_mode):
    cluster = ray_start_cluster_head
    address, env, runtime_env_dir = start_client_server(cluster, client_mode)

    # Unpack lazy fixture tuple to override "working_dir" to fill up
    # execute_statement locals()
    print(working_dir_parametrized)
    working_dir, runtime_env, expected = working_dir_parametrized
    print(working_dir, runtime_env, expected)

    # Setup runtime env here
    runtime_env = f"""{{  "working_dir": "{working_dir}" }}"""
    # Execute the following cmd in driver with runtime_env
    execute_statement = "print(sum(ray.get([run_test.remote()] * 1000)))"
    script = driver_script.format(**locals())

    with tempfile.TemporaryDirectory() as tmp_dir:
        # Execute driver script in brand new, empty directory
        os.chdir(tmp_dir)
        out = run_string_as_driver(script, env)
        assert out.strip().split()[-1] == expected
        assert len(list(Path(working_dir).iterdir())) == 1
        assert len(kv._internal_kv_list("gcs://")) == 0
        # working_dir fixture will take care of going back to original test
        # folder


@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
@pytest.mark.parametrize("client_mode", [True, False])
def test_two_node(two_node_cluster, working_dir_parametrized, client_mode):
    cluster, _ = two_node_cluster
    address, env, runtime_env_dir = start_client_server(cluster, client_mode)
    # Unpack lazy fixture tuple to override "working_dir" to fill up
    # execute_statement locals()
    working_dir, runtime_env, expected = working_dir_parametrized
    # Execute the following cmd in driver with runtime_env
    execute_statement = "print(sum(ray.get([run_test.remote()] * 1000)))"
    script = driver_script.format(**locals())
    with tempfile.TemporaryDirectory() as tmp_dir:
        # Execute driver script in brand new, empty directory
        os.chdir(tmp_dir)
        out = run_string_as_driver(script, env)
        assert out.strip().split()[-1] == expected
        assert len(list(Path(working_dir).iterdir())) == 1
        assert len(kv._internal_kv_list("gcs://")) == 0
        # working_dir fixture will take care of going back to original test
        # folder


@pytest.mark.skip("py_modules not supported yet.")
@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
@pytest.mark.parametrize("client_mode", [True, False])
def test_two_node_module(two_node_cluster, working_dir, client_mode):
    cluster, _ = two_node_cluster
    address, env, runtime_env_dir = start_client_server(cluster, client_mode)
    # test runtime_env iwth py_modules
    runtime_env = """{  "py_modules": [test_module.__path__[0]] }"""
    # Execute the following cmd in driver with runtime_env
    execute_statement = "print(sum(ray.get([run_test.remote()] * 1000)))"
    script = driver_script.format(**locals())
    out = run_string_as_driver(script, env)
    assert out.strip().split()[-1] == "1000"
    assert len(list(Path(runtime_env_dir).iterdir())) == 1


@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
@pytest.mark.parametrize("client_mode", [True, False])
def test_two_node_local_file(two_node_cluster, working_dir, client_mode):
    with open(os.path.join(working_dir, "test_file"), "w") as f:
        f.write("1")
    cluster, _ = two_node_cluster
    address, env, runtime_env_dir = start_client_server(cluster, client_mode)
    # test runtime_env iwth working_dir
    runtime_env = f"""{{  "working_dir": "{working_dir}" }}"""
    # Execute the following cmd in driver with runtime_env
    execute_statement = """
vals = ray.get([check_file.remote('test_file')] * 1000)
print(sum([int(v) for v in vals]))
"""
    script = driver_script.format(**locals())
    out = run_string_as_driver(script, env)
    assert out.strip().split()[-1] == "1000"
    assert len(list(Path(runtime_env_dir).iterdir())) == 1
    assert len(kv._internal_kv_list("gcs://")) == 0


@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
@pytest.mark.parametrize("client_mode", [True, False])
def test_exclusion(ray_start_cluster_head, working_dir, client_mode):
    cluster = ray_start_cluster_head
    address, env, runtime_env_dir = start_client_server(cluster, client_mode)
    working_path = Path(working_dir)

    create_file(working_path / "tmp_dir" / "test_1")
    create_file(working_path / "tmp_dir" / "test_2")
    create_file(working_path / "tmp_dir" / "test_3")
    create_file(working_path / "tmp_dir" / "sub_dir" / "test_1")
    create_file(working_path / "tmp_dir" / "sub_dir" / "test_2")
    create_file(working_path / "test1")
    create_file(working_path / "test2")
    create_file(working_path / "test3")
    tmp_dir_test_3 = str((working_path / "tmp_dir" / "test_3").absolute())
    runtime_env = f"""{{
        "working_dir": r"{working_dir}",
    }}"""
    execute_statement = """
    vals = ray.get([
        check_file.remote('test1'),
        check_file.remote('test2'),
        check_file.remote('test3'),
        check_file.remote(os.path.join('tmp_dir', 'test_1')),
        check_file.remote(os.path.join('tmp_dir', 'test_2')),
        check_file.remote(os.path.join('tmp_dir', 'test_3')),
        check_file.remote(os.path.join('tmp_dir', 'sub_dir', 'test_1')),
        check_file.remote(os.path.join('tmp_dir', 'sub_dir', 'test_2')),
    ])
    print(','.join(vals))
"""
    script = driver_script.format(**locals())
    out = run_string_as_driver(script, env)
    # Test it works before
    assert out.strip().split("\n")[-1] == \
        "Test,Test,Test,Test,Test,Test,Test,Test"
    runtime_env = f"""{{
        "working_dir": r"{working_dir}",
        "excludes": [
            # exclude by relative path
            r"test2",
            # exclude by dir
            r"{str(Path("tmp_dir") / "sub_dir")}",
            # exclude part of the dir
            r"{str(Path("tmp_dir") / "test_1")}",
            # exclude part of the dir
            r"{str(Path("tmp_dir") / "test_2")}",
        ]
    }}"""
    script = driver_script.format(**locals())
    out = run_string_as_driver(script, env)
    assert out.strip().split("\n")[-1] == \
        "Test,FAILED,Test,FAILED,FAILED,Test,FAILED,FAILED"
    # Test excluding all files using gitignore pattern matching syntax
    runtime_env = f"""{{
        "working_dir": r"{working_dir}",
        "excludes": ["*"]
    }}"""
    script = driver_script.format(**locals())
    out = run_string_as_driver(script, env)
    assert out.strip().split("\n")[-1] == \
        "FAILED,FAILED,FAILED,FAILED,FAILED,FAILED,FAILED,FAILED"


@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
@pytest.mark.parametrize("client_mode", [True, False])
def test_exclusion_2(ray_start_cluster_head, working_dir, client_mode):
    cluster = ray_start_cluster_head
    address, env, runtime_env_dir = start_client_server(cluster, client_mode)
    working_path = Path(working_dir)

    def create_file(p):
        if not p.parent.exists():
            p.parent.mkdir(parents=True)
        with p.open("w") as f:
            f.write("Test")

    create_file(working_path / "tmp_dir" / "test_1")
    create_file(working_path / "tmp_dir" / "test_2")
    create_file(working_path / "tmp_dir" / "test_3")
    create_file(working_path / "tmp_dir" / "sub_dir" / "test_1")
    create_file(working_path / "tmp_dir" / "sub_dir" / "test_2")
    create_file(working_path / "test1")
    create_file(working_path / "test2")
    create_file(working_path / "test3")
    create_file(working_path / "cache" / "test_1")
    create_file(working_path / "tmp_dir" / "cache" / "test_1")
    create_file(working_path / "another_dir" / "cache" / "test_1")
    tmp_dir_test_3 = str((working_path / "tmp_dir" / "test_3").absolute())
    runtime_env = f"""{{
        "working_dir": r"{working_dir}",
    }}"""
    execute_statement = """
    vals = ray.get([
        check_file.remote('test1'),
        check_file.remote('test2'),
        check_file.remote('test3'),
        check_file.remote(os.path.join('tmp_dir', 'test_1')),
        check_file.remote(os.path.join('tmp_dir', 'test_2')),
        check_file.remote(os.path.join('tmp_dir', 'test_3')),
        check_file.remote(os.path.join('tmp_dir', 'sub_dir', 'test_1')),
        check_file.remote(os.path.join('tmp_dir', 'sub_dir', 'test_2')),
        check_file.remote(os.path.join("cache", "test_1")),
        check_file.remote(os.path.join("tmp_dir", "cache", "test_1")),
        check_file.remote(os.path.join("another_dir", "cache", "test_1")),
    ])
    print(','.join(vals))
"""
    script = driver_script.format(**locals())
    out = run_string_as_driver(script, env)
    # Test it works before
    assert out.strip().split("\n")[-1] == \
        "Test,Test,Test,Test,Test,Test,Test,Test,Test,Test,Test"
    with open(f"{working_dir}/.gitignore", "w") as f:
        f.write("""
# Comment
test_[12]
/test1
!/tmp_dir/sub_dir/test_1
cache/
""")
    script = driver_script.format(**locals())
    out = run_string_as_driver(script, env)
    t = out.strip().split("\n")[-1]
    assert out.strip().split("\n")[-1] == \
        "FAILED,Test,Test,FAILED,FAILED,Test,Test,FAILED,FAILED,FAILED,FAILED"


@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
@pytest.mark.parametrize("client_mode", [True, False])
def test_runtime_env_getter(ray_start_cluster_head, working_dir, client_mode):
    cluster = ray_start_cluster_head
    address, env, runtime_env_dir = start_client_server(cluster, client_mode)
    runtime_env = f"""{{  "working_dir": "{working_dir}" }}"""
    # Execute the following cmd in driver with runtime_env
    execute_statement = """
print(ray.get_runtime_context().runtime_env["working_dir"])
"""
    script = driver_script.format(**locals())
    out = run_string_as_driver(script, env)
    working_dir_uri = out.strip().split()[-1]
    assert working_dir_uri.startswith("gcs://_ray_pkg_")
    assert working_dir_uri.endswith(".zip")


@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
@pytest.mark.parametrize("client_mode", [True, False])
def test_regular_actors(ray_start_cluster_head, working_dir, client_mode):
    cluster = ray_start_cluster_head
    address, env, runtime_env_dir = start_client_server(cluster, client_mode)
    runtime_env = f"""{{  "working_dir": "{working_dir}" }}"""
    # Execute the following cmd in driver with runtime_env
    execute_statement = """
test_actor = TestActor.options(name="test_actor").remote()
print(sum(ray.get([test_actor.one.remote()] * 1000)))
"""
    script = driver_script.format(**locals())
    out = run_string_as_driver(script, env)
    assert out.strip().split()[-1] == "1000"
    assert len(list(Path(runtime_env_dir).iterdir())) == 1
    assert len(kv._internal_kv_list("gcs://")) == 0


@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
@pytest.mark.parametrize("client_mode", [True, False])
def test_detached_actors(ray_start_cluster_head, working_dir, client_mode):
    cluster = ray_start_cluster_head
    address, env, runtime_env_dir = start_client_server(cluster, client_mode)
    runtime_env = f"""{{  "working_dir": "{working_dir}" }}"""
    # Execute the following cmd in driver with runtime_env
    execute_statement = """
test_actor = TestActor.options(name="test_actor", lifetime="detached").remote()
print(sum(ray.get([test_actor.one.remote()] * 1000)))
"""
    script = driver_script.format(**locals())
    out = run_string_as_driver(script, env)
    assert out.strip().split()[-1] == "1000"
    # It's a detached actors, so it should still be there
    assert len(kv._internal_kv_list("gcs://")) == 1
    assert len(list(Path(runtime_env_dir).iterdir())) == 2
    pkg_dir = [f for f in Path(runtime_env_dir).glob("*") if f.is_dir()][0]
    sys.path.insert(0, str(pkg_dir))
    test_actor = ray.get_actor("test_actor")
    assert sum(ray.get([test_actor.one.remote()] * 1000)) == 1000
    ray.kill(test_actor)
    time.sleep(5)
    assert len(list(Path(runtime_env_dir).iterdir())) == 1
    assert len(kv._internal_kv_list("gcs://")) == 0


@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
def test_util_without_job_config(shutdown_only):
    from ray.cluster_utils import Cluster

    with tempfile.TemporaryDirectory() as tmp_dir:
        with (Path(tmp_dir) / "lib.py").open("w") as f:
            f.write("""
def one():
    return 1
                    """)
        old_dir = os.getcwd()
        os.chdir(tmp_dir)
        cluster = Cluster()
        cluster.add_node(num_cpus=1)
        ray.init(address=cluster.address)
        address, env, runtime_env_dir = start_client_server(cluster, True)
        script = f"""
import ray
import ray.util
import os


ray.util.connect("{address}", job_config=None)

@ray.remote
def run():
    from lib import one
    return one()

print(ray.get([run.remote()])[0])
"""
        out = run_string_as_driver(script, env)
        print(out)
        os.chdir(old_dir)


@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
def test_init(shutdown_only):
    with tempfile.TemporaryDirectory() as tmp_dir:
        old_dir = os.getcwd()
        os.chdir(tmp_dir)
        with open("hello", "w") as f:
            f.write("world")
        ray.init(runtime_env={"working_dir": "."})

        @ray.remote
        class Test:
            def test(self):
                with open("hello") as f:
                    return f.read()

        t = Test.remote()
        assert ray.get(t.test.remote()) == "world"
        os.chdir(old_dir)


def test_get_wheel_filename():
    ray_version = "2.0.0.dev0"
    for sys_platform in ["darwin", "linux", "win32"]:
        for py_version in ["36", "37", "38", "39"]:
            filename = get_wheel_filename(sys_platform, ray_version,
                                          py_version)
            prefix = "https://s3-us-west-2.amazonaws.com/ray-wheels/latest/"
            url = f"{prefix}{filename}"
            assert requests.head(url).status_code == 200, url


def test_get_master_wheel_url():
    ray_version = "2.0.0.dev0"
    test_commit = "58a73821fbfefbf53a19b6c7ffd71e70ccf258c7"
    for sys_platform in ["darwin", "linux", "win32"]:
        for py_version in ["36", "37", "38", "39"]:
            url = get_master_wheel_url(test_commit, sys_platform, ray_version,
                                       py_version)
            assert requests.head(url).status_code == 200, url


def test_get_release_wheel_url():
    test_commits = {"1.6.0": "5052fe67d99f1d4bfc81b2a8694dbf2aa807bbdc"}
    for sys_platform in ["darwin", "linux", "win32"]:
        for py_version in ["36", "37", "38", "39"]:
            for version, commit in test_commits.items():
                url = get_release_wheel_url(commit, sys_platform, version,
                                            py_version)
                assert requests.head(url).status_code == 200, url


@pytest.mark.skipif(
    sys.platform == "win32", reason="runtime_env unsupported on Windows.")
def test_decorator_task(ray_start_cluster_head):
    @ray.remote(runtime_env={"env_vars": {"foo": "bar"}})
    def f():
        return os.environ.get("foo")

    assert ray.get(f.remote()) == "bar"


@pytest.mark.skipif(
    sys.platform == "win32", reason="runtime_env unsupported on Windows.")
def test_decorator_actor(ray_start_cluster_head):
    @ray.remote(runtime_env={"env_vars": {"foo": "bar"}})
    class A:
        def g(self):
            return os.environ.get("foo")

    a = A.remote()
    assert ray.get(a.g.remote()) == "bar"


@pytest.mark.skipif(
    sys.platform == "win32", reason="runtime_env unsupported on Windows.")
def test_decorator_complex(shutdown_only):
    ray.init(
        job_config=ray.job_config.JobConfig(
            runtime_env={"env_vars": {
                "foo": "job"
            }}))

    @ray.remote
    def env_from_job():
        return os.environ.get("foo")

    assert ray.get(env_from_job.remote()) == "job"

    @ray.remote(runtime_env={"env_vars": {"foo": "task"}})
    def f():
        return os.environ.get("foo")

    assert ray.get(f.remote()) == "task"

    @ray.remote(runtime_env={"env_vars": {"foo": "actor"}})
    class A:
        def g(self):
            return os.environ.get("foo")

    a = A.remote()
    assert ray.get(a.g.remote()) == "actor"

    # Test that runtime_env can be overridden by specifying .options().

    assert ray.get(
        f.options(runtime_env={
            "env_vars": {
                "foo": "new"
            }
        }).remote()) == "new"

    a = A.options(runtime_env={"env_vars": {"foo": "new2"}}).remote()
    assert ray.get(a.g.remote()) == "new2"


def test_container_option_serialize():
    runtime_env = {
        "container": {
            "image": "ray:latest",
            "run_options": ["--name=test"]
        }
    }
    job_config = ray.job_config.JobConfig(runtime_env=runtime_env)
    job_config_serialized = job_config.serialize()
    # job_config_serialized is JobConfig protobuf serialized string,
    # job_config.runtime_env.serialized_runtime_env has container_option info
    assert job_config_serialized.count(b"image") == 1


def test_working_dir_override_failure(shutdown_only):
    ray.init()

    with pytest.raises(ValueError):

        @ray.remote(runtime_env={"working_dir": "."})
        def f():
            pass

    @ray.remote
    def g():
        pass

    with pytest.raises(ValueError):
        g.options(runtime_env={"working_dir": "."})

    with pytest.raises(ValueError):

        @ray.remote(runtime_env={"working_dir": "."})
        class A:
            pass

    @ray.remote
    class B:
        pass

    with pytest.raises(ValueError):
        B.options(runtime_env={"working_dir": "."})


@pytest.mark.skipif(
    sys.platform == "win32", reason="runtime_env unsupported on Windows.")
def test_invalid_conda_env(shutdown_only):
    ray.init()

    @ray.remote
    def f():
        pass

    start = time.time()
    bad_env = {"conda": {"dependencies": ["this_doesnt_exist"]}}
    with pytest.raises(RuntimeEnvSetupError):
        ray.get(f.options(runtime_env=bad_env).remote())
    first_time = time.time() - start

    # Check that another valid task can run.
    ray.get(f.remote())

    # The second time this runs it should be faster as the error is cached.
    start = time.time()
    with pytest.raises(RuntimeEnvSetupError):
        ray.get(f.options(runtime_env=bad_env).remote())

    assert (time.time() - start) < (first_time / 2.0)


@pytest.mark.skipif(
    sys.platform == "win32", reason="runtime_env unsupported on Windows.")
def test_no_spurious_worker_startup(shutdown_only):
    """Test that no extra workers start up during a long env installation."""

    # Causes agent to sleep for 15 seconds to simulate creating a runtime env.
    os.environ["RAY_RUNTIME_ENV_SLEEP_FOR_TESTING_S"] = "15"
    ray.init(num_cpus=1)

    @ray.remote
    class Counter(object):
        def __init__(self):
            self.value = 0

        def get(self):
            return self.value

    # Set a nonempty runtime env so that the runtime env setup hook is called.
    runtime_env = {"env_vars": {"a": "b"}}

    # Instantiate an actor that requires the long runtime env installation.
    a = Counter.options(runtime_env=runtime_env).remote()
    assert ray.get(a.get.remote()) == 0

    # Check "debug_state.txt" to ensure no extra workers were started.
    session_dir = ray.worker.global_worker.node.address_info["session_dir"]
    session_path = Path(session_dir)
    debug_state_path = session_path / "debug_state.txt"

    def get_num_workers():
        with open(debug_state_path) as f:
            for line in f.readlines():
                num_workers_prefix = "- num PYTHON workers: "
                if num_workers_prefix in line:
                    return int(line[len(num_workers_prefix):])
        return None

    # Wait for "debug_state.txt" to be updated to reflect the started worker.
    start = time.time()
    wait_for_condition(
        lambda: get_num_workers() is not None and get_num_workers() > 0)
    time_waited = time.time() - start
    print(f"Waited {time_waited} for debug_state.txt to be updated")

    # If any workers were unnecessarily started during the initial env
    # installation, they will bypass the runtime env setup hook (because the
    # created env will have been cached) and should be added to num_workers
    # within a few seconds.  Adjusting the default update period for
    # debut_state.txt via this cluster_utils pytest fixture seems to be broken,
    # so just check it for the next 10 seconds (the default period).
    start = time.time()
    got_num_workers = False
    while time.time() - start < 10:
        # Check that no more workers were started.
        num_workers = get_num_workers()
        if num_workers is not None:
            got_num_workers = True
            assert num_workers <= 1
        time.sleep(0.1)
    assert got_num_workers, "failed to read num workers for 10 seconds"


@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
def test_large_file_boundary(shutdown_only):
    with tempfile.TemporaryDirectory() as tmp_dir:
        old_dir = os.getcwd()
        os.chdir(tmp_dir)

        # Check that packages just under the max size work as expected.
        size = GCS_STORAGE_MAX_SIZE - 1024 * 1024
        with open("test_file", "wb") as f:
            f.write(os.urandom(size))

        ray.init(runtime_env={"working_dir": "."})

        @ray.remote
        class Test:
            def get_size(self):
                with open("test_file", "rb") as f:
                    return len(f.read())

        t = Test.remote()
        assert ray.get(t.get_size.remote()) == size
        os.chdir(old_dir)


@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
def test_large_file_error(shutdown_only):
    with tempfile.TemporaryDirectory() as tmp_dir:
        old_dir = os.getcwd()
        os.chdir(tmp_dir)

        # Write to two separate files, each of which is below the threshold to
        # make sure the error is for the full package size.
        size = GCS_STORAGE_MAX_SIZE // 2 + 1
        with open("test_file_1", "wb") as f:
            f.write(os.urandom(size))

        with open("test_file_2", "wb") as f:
            f.write(os.urandom(size))

        with pytest.raises(RuntimeError):
            ray.init(runtime_env={"working_dir": "."})

        os.chdir(old_dir)


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-sv", __file__]))
