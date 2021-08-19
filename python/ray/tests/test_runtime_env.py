import os
import pytest
import sys
import random
import tempfile
import time
import requests
from pathlib import Path

import ray
from ray.exceptions import RuntimeEnvSetupError
from ray.test_utils import (
    run_string_as_driver, run_string_as_driver_nonblocking, wait_for_condition)
from ray._private.utils import (get_wheel_filename, get_master_wheel_url,
                                get_release_wheel_url)
import ray.experimental.internal_kv as kv
from time import sleep
driver_script = """
from time import sleep
import sys
import logging
sys.path.insert(0, "{working_dir}")
import ray
import ray.util
import os

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
        ray.client("{address}").env({runtime_env}).namespace("").connect()
    else:
        ray.init(address="{address}",
                 job_config=job_config,
                 logging_level=logging.DEBUG,
                 namespace=""
)
except ValueError:
    print("ValueError")
    sys.exit(0)
except TypeError:
    print("TypeError")
    sys.exit(0)
except:
    print("ERROR")
    sys.exit(0)


if os.environ.get("EXIT_AFTER_INIT"):
    sys.exit(0)

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
sleep(10)
"""


def create_file(p):
    if not p.parent.exists():
        p.parent.mkdir()
    with p.open("w") as f:
        f.write("Test")


@pytest.fixture(scope="function")
def working_dir():
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


def start_client_server(cluster, client_mode):
    from ray._private.runtime_env import PKG_DIR
    if not client_mode:
        return (cluster.address, {}, PKG_DIR)
    ray.worker._global_node._ray_params.ray_client_server_port = "10003"
    ray.worker._global_node.start_ray_client_server()
    return ("localhost:10003", {"USE_RAY_CLIENT": "1"}, PKG_DIR)


@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
def test_travel():
    import uuid
    with tempfile.TemporaryDirectory() as tmp_dir:
        dir_paths = set()
        file_paths = set()
        item_num = 0
        excludes = []
        root = Path(tmp_dir) / "test"

        def construct(path, excluded=False, depth=0):
            nonlocal item_num
            path.mkdir(parents=True)
            if not excluded:
                dir_paths.add(str(path))
            if depth > 8:
                return
            if item_num > 500:
                return
            dir_num = random.randint(0, 10)
            file_num = random.randint(0, 10)
            for _ in range(dir_num):
                uid = str(uuid.uuid4()).split("-")[0]
                dir_path = path / uid
                exclud_sub = random.randint(0, 5) == 0
                if not excluded and exclud_sub:
                    excludes.append(str(dir_path.relative_to(root)))
                if not excluded:
                    construct(dir_path, exclud_sub or excluded, depth + 1)
                item_num += 1
            if item_num > 1000:
                return

            for _ in range(file_num):
                uid = str(uuid.uuid4()).split("-")[0]
                with (path / uid).open("w") as f:
                    v = random.randint(0, 1000)
                    f.write(str(v))
                    if not excluded:
                        if random.randint(0, 5) == 0:
                            excludes.append(
                                str((path / uid).relative_to(root)))
                        else:
                            file_paths.add((str(path / uid), str(v)))
                item_num += 1

        construct(root)
        exclude_spec = ray._private.runtime_env._get_excludes(root, excludes)
        visited_dir_paths = set()
        visited_file_paths = set()

        def handler(path):
            if path.is_dir():
                visited_dir_paths.add(str(path))
            else:
                with open(path) as f:
                    visited_file_paths.add((str(path), f.read()))

        ray._private.runtime_env._dir_travel(root, [exclude_spec], handler)
        assert file_paths == visited_file_paths
        assert dir_paths == visited_dir_paths


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
    (address, env, PKG_DIR) = start_client_server(cluster, client_mode)
    env["EXIT_AFTER_INIT"] = "1"
    with tempfile.TemporaryDirectory() as working_dir:
        runtime_env = f"""{{
    "working_dir": r"{working_dir}",
    "py_modules": [r"{working_dir}"]
}}"""
        # Execute the following cmd in driver with runtime_env
        execute_statement = "sys.exit(0)"
        script = driver_script.format(**locals())
        out = run_string_as_driver(script, env)
        assert out != "ERROR"


@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
@pytest.mark.parametrize("client_mode", [True, False])
def test_invalid_working_dir(ray_start_cluster_head, working_dir, client_mode):
    cluster = ray_start_cluster_head
    (address, env, PKG_DIR) = start_client_server(cluster, client_mode)
    env["EXIT_AFTER_INIT"] = "1"

    runtime_env = "{ 'working_dir': 10 }"
    # Execute the following cmd in driver with runtime_env
    execute_statement = ""
    script = driver_script.format(**locals())
    out = run_string_as_driver(script, env).strip().split()[-1]
    assert out == "TypeError"

    runtime_env = "{ 'py_modules': [10] }"
    # Execute the following cmd in driver with runtime_env
    execute_statement = ""
    script = driver_script.format(**locals())
    out = run_string_as_driver(script, env).strip().split()[-1]
    assert out == "TypeError"

    runtime_env = f"{{ 'working_dir': os.path.join(r'{working_dir}', 'na') }}"
    # Execute the following cmd in driver with runtime_env
    execute_statement = ""
    script = driver_script.format(**locals())
    out = run_string_as_driver(script, env).strip().split()[-1]
    assert out == "ValueError"

    runtime_env = f"{{ 'py_modules': [os.path.join(r'{working_dir}', 'na')] }}"
    # Execute the following cmd in driver with runtime_env
    execute_statement = ""
    script = driver_script.format(**locals())
    out = run_string_as_driver(script, env).strip().split()[-1]
    assert out == "ValueError"


@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
@pytest.mark.parametrize("client_mode", [True, False])
def test_single_node(ray_start_cluster_head, working_dir, client_mode):
    cluster = ray_start_cluster_head
    (address, env, PKG_DIR) = start_client_server(cluster, client_mode)
    # Setup runtime env here
    runtime_env = f"""{{  "working_dir": "{working_dir}" }}"""
    # Execute the following cmd in driver with runtime_env
    execute_statement = "print(sum(ray.get([run_test.remote()] * 1000)))"
    script = driver_script.format(**locals())
    out = run_string_as_driver(script, env)
    assert out.strip().split()[-1] == "1000"
    assert len(list(Path(PKG_DIR).iterdir())) == 1
    assert len(kv._internal_kv_list("gcs://")) == 0


@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
@pytest.mark.parametrize("client_mode", [True, False])
def test_two_node(two_node_cluster, working_dir, client_mode):
    cluster, _ = two_node_cluster
    (address, env, PKG_DIR) = start_client_server(cluster, client_mode)
    # Testing runtime env with working_dir
    runtime_env = f"""{{  "working_dir": "{working_dir}" }}"""
    # Execute the following cmd in driver with runtime_env
    execute_statement = "print(sum(ray.get([run_test.remote()] * 1000)))"
    script = driver_script.format(**locals())
    out = run_string_as_driver(script, env)
    assert out.strip().split()[-1] == "1000"
    assert len(list(Path(PKG_DIR).iterdir())) == 1
    assert len(kv._internal_kv_list("gcs://")) == 0


@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
@pytest.mark.parametrize("client_mode", [True, False])
def test_two_node_module(two_node_cluster, working_dir, client_mode):
    cluster, _ = two_node_cluster
    (address, env, PKG_DIR) = start_client_server(cluster, client_mode)
    # test runtime_env iwth py_modules
    runtime_env = """{  "py_modules": [test_module.__path__[0]] }"""
    # Execute the following cmd in driver with runtime_env
    execute_statement = "print(sum(ray.get([run_test.remote()] * 1000)))"
    script = driver_script.format(**locals())
    out = run_string_as_driver(script, env)
    assert out.strip().split()[-1] == "1000"
    assert len(list(Path(PKG_DIR).iterdir())) == 1


@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
@pytest.mark.parametrize("client_mode", [True, False])
def test_two_node_local_file(two_node_cluster, working_dir, client_mode):
    with open(os.path.join(working_dir, "test_file"), "w") as f:
        f.write("1")
    cluster, _ = two_node_cluster
    (address, env, PKG_DIR) = start_client_server(cluster, client_mode)
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
    assert len(list(Path(PKG_DIR).iterdir())) == 1
    assert len(kv._internal_kv_list("gcs://")) == 0


@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
@pytest.mark.parametrize("client_mode", [True, False])
def test_exclusion(ray_start_cluster_head, working_dir, client_mode):
    cluster = ray_start_cluster_head
    (address, env, PKG_DIR) = start_client_server(cluster, client_mode)
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


@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
@pytest.mark.parametrize("client_mode", [True, False])
def test_exclusion_2(ray_start_cluster_head, working_dir, client_mode):
    cluster = ray_start_cluster_head
    (address, env, PKG_DIR) = start_client_server(cluster, client_mode)
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
    (address, env, PKG_DIR) = start_client_server(cluster, client_mode)
    runtime_env = f"""{{  "working_dir": "{working_dir}" }}"""
    # Execute the following cmd in driver with runtime_env
    execute_statement = """
print(ray.get_runtime_context().runtime_env["working_dir"])
"""
    script = driver_script.format(**locals())
    out = run_string_as_driver(script, env)
    assert out.strip().split()[-1] == working_dir


@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
@pytest.mark.parametrize("client_mode", [True, False])
def test_two_node_uri(two_node_cluster, working_dir, client_mode):
    cluster, _ = two_node_cluster
    (address, env, PKG_DIR) = start_client_server(cluster, client_mode)
    import ray._private.runtime_env as runtime_env
    import tempfile
    with tempfile.NamedTemporaryFile(suffix="zip") as tmp_file:
        pkg_name = runtime_env.get_project_package_name(working_dir, [], [])
        pkg_uri = runtime_env.Protocol.PIN_GCS.value + "://" + pkg_name
        runtime_env.create_project_package(working_dir, [], [], tmp_file.name)
        runtime_env.push_package(pkg_uri, tmp_file.name)
        runtime_env = f"""{{ "uris": ["{pkg_uri}"] }}"""
        # Execute the following cmd in driver with runtime_env
        execute_statement = "print(sum(ray.get([run_test.remote()] * 1000)))"
    script = driver_script.format(**locals())
    out = run_string_as_driver(script, env)
    assert out.strip().split()[-1] == "1000"
    assert len(list(Path(PKG_DIR).iterdir())) == 1
    # pinned uri will not be deleted
    print(list(kv._internal_kv_list("")))
    assert len(kv._internal_kv_list("pingcs://")) == 1


@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
@pytest.mark.parametrize("client_mode", [True, False])
def test_regular_actors(ray_start_cluster_head, working_dir, client_mode):
    cluster = ray_start_cluster_head
    (address, env, PKG_DIR) = start_client_server(cluster, client_mode)
    runtime_env = f"""{{  "working_dir": "{working_dir}" }}"""
    # Execute the following cmd in driver with runtime_env
    execute_statement = """
test_actor = TestActor.options(name="test_actor").remote()
print(sum(ray.get([test_actor.one.remote()] * 1000)))
"""
    script = driver_script.format(**locals())
    out = run_string_as_driver(script, env)
    assert out.strip().split()[-1] == "1000"
    assert len(list(Path(PKG_DIR).iterdir())) == 1
    assert len(kv._internal_kv_list("gcs://")) == 0


@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
@pytest.mark.parametrize("client_mode", [True, False])
def test_detached_actors(ray_start_cluster_head, working_dir, client_mode):
    cluster = ray_start_cluster_head
    (address, env, PKG_DIR) = start_client_server(cluster, client_mode)
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
    assert len(list(Path(PKG_DIR).iterdir())) == 2
    pkg_dir = [f for f in Path(PKG_DIR).glob("*") if f.is_dir()][0]
    import sys
    sys.path.insert(0, str(pkg_dir))
    test_actor = ray.get_actor("test_actor")
    assert sum(ray.get([test_actor.one.remote()] * 1000)) == 1000
    ray.kill(test_actor)
    from time import sleep
    sleep(5)
    assert len(list(Path(PKG_DIR).iterdir())) == 1
    assert len(kv._internal_kv_list("gcs://")) == 0


@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
def test_jobconfig_compatible_1(ray_start_cluster_head, working_dir):
    # start job_config=None
    # start job_config=something
    cluster = ray_start_cluster_head
    (address, env, PKG_DIR) = start_client_server(cluster, True)
    runtime_env = None
    # To make the first one hanging there
    execute_statement = """
sleep(600)
"""
    script = driver_script.format(**locals())
    # Have one running with job config = None
    proc = run_string_as_driver_nonblocking(script, env)
    # waiting it to be up
    sleep(5)
    runtime_env = f"""{{  "working_dir": "{working_dir}" }}"""
    # Execute the second one which should work because Ray Client servers.
    execute_statement = "print(sum(ray.get([run_test.remote()] * 1000)))"
    script = driver_script.format(**locals())
    out = run_string_as_driver(script, env)
    assert out.strip().split()[-1] == "1000"
    proc.kill()
    proc.wait()


@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
def test_jobconfig_compatible_2(ray_start_cluster_head, working_dir):
    # start job_config=something
    # start job_config=None
    cluster = ray_start_cluster_head
    (address, env, PKG_DIR) = start_client_server(cluster, True)
    runtime_env = """{  "py_modules": [test_module.__path__[0]] }"""
    # To make the first one hanging there
    execute_statement = """
sleep(600)
"""
    script = driver_script.format(**locals())
    proc = run_string_as_driver_nonblocking(script, env)
    sleep(5)
    runtime_env = None
    # Execute the following in the second one which should
    # succeed
    execute_statement = "print('OK')"
    script = driver_script.format(**locals())
    out = run_string_as_driver(script, env)
    assert out.strip().split()[-1] == "OK", out
    proc.kill()
    proc.wait()


@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
def test_jobconfig_compatible_3(ray_start_cluster_head, working_dir):
    # start job_config=something
    # start job_config=something else
    cluster = ray_start_cluster_head
    (address, env, PKG_DIR) = start_client_server(cluster, True)
    runtime_env = """{  "py_modules": [test_module.__path__[0]] }"""
    # To make the first one hanging ther
    execute_statement = """
sleep(600)
"""
    script = driver_script.format(**locals())
    proc = run_string_as_driver_nonblocking(script, env)
    sleep(5)
    runtime_env = f"""
{{  "working_dir": test_module.__path__[0] }}"""  # noqa: F541
    # Execute the following cmd in the second one and ensure that
    # it is able to run.
    execute_statement = "print('OK')"
    script = driver_script.format(**locals())
    out = run_string_as_driver(script, env)
    proc.kill()
    proc.wait()
    assert out.strip().split()[-1] == "OK"


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
        (address, env, PKG_DIR) = start_client_server(cluster, True)
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
        for py_version in ["36", "37", "38"]:
            filename = get_wheel_filename(sys_platform, ray_version,
                                          py_version)
            prefix = "https://s3-us-west-2.amazonaws.com/ray-wheels/latest/"
            url = f"{prefix}{filename}"
            assert requests.head(url).status_code == 200


def test_get_master_wheel_url():
    ray_version = "2.0.0.dev0"
    test_commit = "58a73821fbfefbf53a19b6c7ffd71e70ccf258c7"
    for sys_platform in ["darwin", "linux", "win32"]:
        for py_version in ["36", "37", "38"]:
            url = get_master_wheel_url(test_commit, sys_platform, ray_version,
                                       py_version)
            assert requests.head(url).status_code == 200, url


def test_get_release_wheel_url():
    test_commits = {"1.6.0": "5052fe67d99f1d4bfc81b2a8694dbf2aa807bbdc"}
    for sys_platform in ["darwin", "linux", "win32"]:
        for py_version in ["36", "37", "38"]:
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
    # job_config.runtime_env.raw_json has container_option info
    # job_config.serialized_runtime_env also has container_option info
    assert job_config_serialized.count(b"image") == 2


def test_working_dir_override_failure(shutdown_only):
    ray.init()

    @ray.remote(runtime_env={"working_dir": "."})
    def f():
        pass

    with pytest.raises(NotImplementedError):
        f.remote()

    @ray.remote
    def g():
        pass

    with pytest.raises(NotImplementedError):
        g.options(runtime_env={"working_dir": "."}).remote()

    @ray.remote(runtime_env={"working_dir": "."})
    class A:
        pass

    with pytest.raises(NotImplementedError):
        A.remote()

    @ray.remote
    class B:
        pass

    with pytest.raises(NotImplementedError):
        B.options(runtime_env={"working_dir": "."}).remote()


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
@pytest.mark.parametrize(
    "ray_start_cluster", [{
        "_system_config": {
            "event_stats_print_interval_ms": 100,
            "debug_dump_period_milliseconds": 100,
            "event_stats": True
        }
    }],
    indirect=True)
def test_no_spurious_worker_startup(ray_start_cluster):
    """Test that no extra workers start up during a long env installation."""

    cluster = ray_start_cluster

    # This hook sleeps for 15 seconds to simulate creating a runtime env.
    cluster.add_node(
        num_cpus=1,
        runtime_env_setup_hook="ray.test_utils.sleep_setup_runtime_env")

    # Set a nonempty runtime env so that the runtime env setup hook is called.
    runtime_env = {"env_vars": {"a": "b"}}
    ray.init(address=cluster.address)

    @ray.remote
    class Counter(object):
        def __init__(self):
            self.value = 0

        def get(self):
            return self.value

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
    wait_for_condition(lambda: get_num_workers() > 0)
    time_waited = time.time() - start
    print(f"Waited {time_waited} for debug_state.txt to be updated")

    # If any workers were unnecessarily started during the initial env
    # installation, they will bypass the runtime env setup hook because the
    # created env will have been cached and should be added to num_workers
    # within a few seconds.  Adjusting the default update period for
    # debut_state.txt via this cluster_utils pytest fixture seems to be broken,
    # so just check it for the next 10 seconds (the default period).
    for i in range(100):
        # Check that no more workers were started.
        assert get_num_workers() <= 1
        time.sleep(0.1)


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-sv", __file__]))
