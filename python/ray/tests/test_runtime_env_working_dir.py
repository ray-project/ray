from contextlib import contextmanager
import os
from pathlib import Path
import sys
import tempfile

import pytest
from pytest_lazyfixture import lazy_fixture

import ray
import ray.experimental.internal_kv as kv
from ray._private.test_utils import wait_for_condition
from ray._private.runtime_env.packaging import GCS_STORAGE_MAX_SIZE

S3_PACKAGE_URI = "s3://runtime-env-test/remote_runtime_env.zip"


@pytest.fixture(scope="function", params=["ray_client", "no_ray_client"])
def start_cluster(ray_start_cluster, request):
    assert request.param in {"ray_client", "no_ray_client"}
    use_ray_client: bool = request.param == "ray_client"

    cluster = ray_start_cluster
    cluster.add_node(num_cpus=4)
    if use_ray_client:
        cluster.head_node._ray_params.ray_client_server_port = "10003"
        cluster.head_node.start_ray_client_server()
        address = "ray://localhost:10003"
    else:
        address = cluster.address

    yield cluster, address


@pytest.fixture(scope="function")
def tmp_working_dir():
    with tempfile.TemporaryDirectory() as tmp_dir:
        path = Path(tmp_dir)

        hello_file = path / "hello"
        with hello_file.open(mode="w") as f:
            f.write("world")

        module_path = path / "test_module"
        module_path.mkdir(parents=True)

        test_file = module_path / "test.py"
        with test_file.open(mode="w") as f:
            f.write("def one():\n")
            f.write("    return 1\n")

        init_file = module_path / "__init__.py"
        with init_file.open(mode="w") as f:
            f.write("from test_module.test import one\n")

        yield tmp_dir


@pytest.mark.parametrize("test_failure", [True, False])
@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
def test_lazy_reads(start_cluster, tmp_working_dir, test_failure):
    """Tests the case where we lazily read files or import inside a task/actor.

    This tests both that this fails *without* the working_dir and that it
    passes with it.
    """
    cluster, address = start_cluster

    if test_failure:
        # Don't pass working_dir, so it should fail!
        ray.init(address)
    else:
        ray.init(address, runtime_env={"working_dir": tmp_working_dir})

    @ray.remote
    def test_import():
        import test_module
        return test_module.one()

    if test_failure:
        with pytest.raises(ImportError):
            ray.get(test_import.remote())
    else:
        assert ray.get(test_import.remote()) == 1

    @ray.remote
    def test_read():
        return open("hello").read()

    if test_failure:
        with pytest.raises(FileNotFoundError):
            ray.get(test_read.remote())
    else:
        assert ray.get(test_read.remote()) == "world"

    @ray.remote
    class Actor:
        def test_import(self):
            import test_module
            return test_module.one()

        def test_read(self):
            return open("hello").read()

    a = Actor.remote()
    if test_failure:
        with pytest.raises(ImportError):
            assert ray.get(a.test_import.remote()) == 1
        with pytest.raises(FileNotFoundError):
            assert ray.get(a.test_read.remote()) == "world"
    else:
        assert ray.get(a.test_import.remote()) == 1
        assert ray.get(a.test_read.remote()) == "world"


@pytest.mark.parametrize("test_failure", [True, False])
@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
def test_captured_import(start_cluster, tmp_working_dir, test_failure):
    """Tests importing a module in the driver and capturing it in a task/actor.

    This tests both that this fails *without* the working_dir and that it
    passes with it.
    """
    cluster, address = start_cluster

    if test_failure:
        # Don't pass working_dir, so it should fail!
        ray.init(address)
    else:
        ray.init(address, runtime_env={"working_dir": tmp_working_dir})

    # Import in the driver.
    sys.path.insert(0, tmp_working_dir)
    import test_module

    @ray.remote
    def test_import():
        return test_module.one()

    if test_failure:
        with pytest.raises(Exception):
            ray.get(test_import.remote())
    else:
        assert ray.get(test_import.remote()) == 1

    @ray.remote
    class Actor:
        def test_import(self):
            return test_module.one()

        def test_read(self):
            return open("hello").read()

    if test_failure:
        with pytest.raises(Exception):
            a = Actor.remote()
            assert ray.get(a.test_import.remote()) == 1
    else:
        a = Actor.remote()
        assert ray.get(a.test_import.remote()) == 1


@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
def test_empty_working_dir(start_cluster):
    """Tests the case where we pass an empty directory as the working_dir."""
    cluster, address = start_cluster
    with tempfile.TemporaryDirectory() as working_dir:
        ray.init(address, runtime_env={"working_dir": working_dir})

        @ray.remote
        def listdir():
            return os.listdir()

        assert len(ray.get(listdir.remote())) == 0

        @ray.remote
        class A:
            def listdir(self):
                return os.listdir()
                pass

        a = A.remote()
        assert len(ray.get(a.listdir.remote())) == 0


@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
def test_invalid_working_dir(start_cluster):
    """Tests input validation for the working_dir."""
    cluster, address = start_cluster

    with pytest.raises(TypeError):
        ray.init(address, runtime_env={"working_dir": 10})

    ray.shutdown()

    with pytest.raises(ValueError):
        ray.init(address, runtime_env={"working_dir": "/does/not/exist"})

    ray.shutdown()

    with pytest.raises(ValueError):
        ray.init(address, runtime_env={"working_dir": "does_not_exist"})

    ray.shutdown()

    with pytest.raises(ValueError):
        ray.init(address, runtime_env={"working_dir": "s3://no_dot_zip"})


@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
@pytest.mark.parametrize("test_failure", [True, False])
@pytest.mark.parametrize("per_task_actor", [True, False])
def test_s3_uri(start_cluster, test_failure, per_task_actor):
    """Tests the case where we lazily read files or import inside a task/actor.

    In this case, the files come from an S3 URI.

    This tests both that this fails *without* the working_dir and that it
    passes with it.
    """
    cluster, address = start_cluster

    env = {"working_dir": S3_PACKAGE_URI}
    if test_failure or per_task_actor:
        ray.init(address)
    else:
        ray.init(address, runtime_env=env)

    @ray.remote
    def test_import():
        import test_module
        return test_module.one()

    if not test_failure and per_task_actor:
        test_import = test_import.options(runtime_env=env)

    if test_failure:
        with pytest.raises(ImportError):
            ray.get(test_import.remote())
    else:
        assert ray.get(test_import.remote()) == 2

    @ray.remote
    class Actor:
        def test_import(self):
            import test_module
            return test_module.one()

    if not test_failure and per_task_actor:
        Actor = Actor.options(runtime_env=env)

    a = Actor.remote()
    if test_failure:
        with pytest.raises(ImportError):
            assert ray.get(a.test_import.remote()) == 2
    else:
        assert ray.get(a.test_import.remote()) == 2


@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
@pytest.mark.parametrize(
    "working_dir",
    [S3_PACKAGE_URI, lazy_fixture("tmp_working_dir")])
def test_multi_node(start_cluster, working_dir):
    """Tests that the working_dir is propagated across multi-node clusters."""
    NUM_NODES = 3
    cluster, address = start_cluster
    for _ in range(NUM_NODES - 1):  # Head node already added.
        cluster.add_node(num_cpus=1)

    ray.init(address, runtime_env={"working_dir": working_dir})

    @ray.remote(num_cpus=1)
    class A:
        def check_and_get_node_id(self):
            import test_module
            test_module.one()
            return ray.get_runtime_context().node_id

    num_cpus = int(ray.available_resources()["CPU"])
    actors = [A.remote() for _ in range(num_cpus)]
    object_refs = [a.check_and_get_node_id.remote() for a in actors]
    assert len(set(ray.get(object_refs))) == NUM_NODES


def check_internal_kv_gced():
    return len(kv._internal_kv_list("gcs://")) == 0


def check_local_files_gced(cluster):
    for node in cluster.list_all_nodes():
        all_files = os.listdir(node.get_runtime_env_dir_path())
        # Check that there are no files remaining except for .lock files.
        # TODO(edoakes): the lock files should get cleaned up too!
        if len(list(filter(lambda f: not f.endswith(".lock"), all_files))) > 0:
            print(all_files)
            return False

    return True


@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
@pytest.mark.parametrize(
    "working_dir",
    [S3_PACKAGE_URI, lazy_fixture("tmp_working_dir")])
def test_job_level_gc(start_cluster, working_dir):
    """Tests that job-level working_dir is GC'd when the job exits."""
    NUM_NODES = 3
    cluster, address = start_cluster
    for _ in range(NUM_NODES - 1):  # Head node already added.
        cluster.add_node(num_cpus=1)

    ray.init(address, runtime_env={"working_dir": working_dir})

    # For a local directory, the package should be in the GCS.
    # For an S3 URI, there should be nothing in the GCS because
    # it will be downloaded from S3 directly on each node.
    if working_dir == S3_PACKAGE_URI:
        assert check_internal_kv_gced()
    else:
        assert not check_internal_kv_gced()

    @ray.remote(num_cpus=1)
    class A:
        def test_import(self):
            import test_module
            test_module.one()

    num_cpus = int(ray.available_resources()["CPU"])
    actors = [A.remote() for _ in range(num_cpus)]
    ray.get([a.test_import.remote() for a in actors])

    if working_dir == S3_PACKAGE_URI:
        assert check_internal_kv_gced()
    else:
        assert not check_internal_kv_gced()
    assert not check_local_files_gced(cluster)

    ray.shutdown()

    # Need to re-connect to use internal_kv.
    ray.init(address=address)
    wait_for_condition(check_internal_kv_gced)
    wait_for_condition(lambda: check_local_files_gced(cluster))


# TODO(edoakes): fix this bug and enable test.
@pytest.mark.skip("Currently failing.")
@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
def test_actor_level_gc(start_cluster):
    """Tests that actor-level working_dir is GC'd when the actor exits."""
    NUM_NODES = 3
    cluster, address = start_cluster
    for _ in range(NUM_NODES - 1):  # Head node already added.
        cluster.add_node(num_cpus=1)

    ray.init(address)

    @ray.remote
    class A:
        def check(self):
            assert "test_module" in os.listdir()

    # TODO(edoakes): this doesn't work in decorator with ray client.
    A = A.options(runtime_env={"working_dir": S3_PACKAGE_URI})

    actors = [A.remote() for _ in range(5)]
    ray.get([a.check.remote() for a in actors])

    assert not check_local_files_gced(cluster)

    [ray.kill(a) for a in actors]

    wait_for_condition(lambda: check_local_files_gced(cluster))


@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
@pytest.mark.parametrize(
    "working_dir",
    [S3_PACKAGE_URI, lazy_fixture("tmp_working_dir")])
def test_detached_actor_gc(start_cluster, working_dir):
    """Tests that URIs for detached actors are GC'd only when they exit."""
    cluster, address = start_cluster
    ray.init(
        address, namespace="test", runtime_env={"working_dir": working_dir})

    # For a local directory, the package should be in the GCS.
    # For an S3 URI, there should be nothing in the GCS because
    # it will be downloaded from S3 directly on each node.
    if working_dir == S3_PACKAGE_URI:
        assert check_internal_kv_gced()
    else:
        assert not check_internal_kv_gced()

    @ray.remote
    class A:
        def test_import(self):
            import test_module
            test_module.one()

    a = A.options(name="test", lifetime="detached").remote()
    ray.get(a.test_import.remote())

    if working_dir == S3_PACKAGE_URI:
        assert check_internal_kv_gced()
    else:
        assert not check_internal_kv_gced()
    assert not check_local_files_gced(cluster)

    ray.shutdown()

    ray.init(address, namespace="test")

    if working_dir == S3_PACKAGE_URI:
        assert check_internal_kv_gced()
    else:
        assert not check_internal_kv_gced()
    assert not check_local_files_gced(cluster)

    a = ray.get_actor("test")
    ray.get(a.test_import.remote())

    ray.kill(a)
    wait_for_condition(check_internal_kv_gced)
    wait_for_condition(lambda: check_local_files_gced(cluster))


@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
def test_exclusion(start_cluster, tmp_working_dir):
    """Tests various forms of the 'excludes' parameter."""
    cluster, address = start_cluster

    def create_file(p):
        if not p.parent.exists():
            p.parent.mkdir(parents=True)
        with p.open("w") as f:
            f.write("Test")

    working_path = Path(tmp_working_dir)
    create_file(working_path / "test1")
    create_file(working_path / "test2")
    create_file(working_path / "test3")
    create_file(working_path / "tmp_dir" / "test_1")
    create_file(working_path / "tmp_dir" / "test_2")
    create_file(working_path / "tmp_dir" / "test_3")
    create_file(working_path / "tmp_dir" / "sub_dir" / "test_1")
    create_file(working_path / "tmp_dir" / "sub_dir" / "test_2")
    create_file(working_path / "cache" / "test_1")
    create_file(working_path / "tmp_dir" / "cache" / "test_1")
    create_file(working_path / "another_dir" / "cache" / "test_1")

    # Test that all files are present without excluding.
    ray.init(address, runtime_env={"working_dir": tmp_working_dir})

    @ray.remote
    def check_file(name):
        try:
            with open(name) as f:
                return f.read()
        except Exception:
            return "FAILED"

    def get_all():
        return ray.get([
            check_file.remote("test1"),
            check_file.remote("test2"),
            check_file.remote("test3"),
            check_file.remote(os.path.join("tmp_dir", "test_1")),
            check_file.remote(os.path.join("tmp_dir", "test_2")),
            check_file.remote(os.path.join("tmp_dir", "test_3")),
            check_file.remote(os.path.join("tmp_dir", "sub_dir", "test_1")),
            check_file.remote(os.path.join("tmp_dir", "sub_dir", "test_2")),
            check_file.remote(os.path.join("cache", "test_1")),
            check_file.remote(os.path.join("tmp_dir", "cache", "test_1")),
            check_file.remote(os.path.join("another_dir", "cache", "test_1")),
        ])

    assert get_all() == [
        "Test", "Test", "Test", "Test", "Test", "Test", "Test", "Test", "Test",
        "Test", "Test"
    ]

    ray.shutdown()

    # Test various exclusion methods.
    ray.init(
        address,
        runtime_env={
            "working_dir": tmp_working_dir,
            "excludes": [
                # exclude by relative path
                "test2",
                # exclude by dir
                str(Path("tmp_dir") / "sub_dir"),
                # exclude part of the dir
                str(Path("tmp_dir") / "test_1"),
                # exclude part of the dir
                str(Path("tmp_dir") / "test_2"),
            ]
        })

    assert get_all() == [
        "Test", "FAILED", "Test", "FAILED", "FAILED", "Test", "FAILED",
        "FAILED", "Test", "Test", "Test"
    ]

    ray.shutdown()

    # Test excluding all files using gitignore pattern matching syntax
    ray.init(
        address,
        runtime_env={
            "working_dir": tmp_working_dir,
            "excludes": ["*"]
        })

    assert get_all() == [
        "FAILED", "FAILED", "FAILED", "FAILED", "FAILED", "FAILED", "FAILED",
        "FAILED", "FAILED", "FAILED", "FAILED"
    ]

    ray.shutdown()

    # Test excluding with a .gitignore file.
    with open(f"{tmp_working_dir}/.gitignore", "w") as f:
        f.write("""
# Comment
test_[12]
/test1
!/tmp_dir/sub_dir/test_1
cache/
""")

    ray.init(
        address, runtime_env={
            "working_dir": tmp_working_dir,
        })

    assert get_all() == [
        "FAILED", "Test", "Test", "FAILED", "FAILED", "Test", "Test", "FAILED",
        "FAILED", "FAILED", "FAILED"
    ]


@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
@pytest.mark.parametrize(
    "working_dir",
    [S3_PACKAGE_URI, lazy_fixture("tmp_working_dir")])
def test_runtime_context(start_cluster, working_dir):
    """Tests that the working_dir is propagated in the runtime_context."""
    cluster, address = start_cluster
    ray.init(runtime_env={"working_dir": working_dir})

    def check():
        wd = ray.get_runtime_context().runtime_env.working_dir
        if working_dir == S3_PACKAGE_URI:
            assert wd == S3_PACKAGE_URI
        else:
            assert wd.startswith("gcs://_ray_pkg_")

    check()

    @ray.remote
    def task():
        check()

    ray.get(task.remote())

    @ray.remote
    class Actor:
        def check(self):
            check()

    a = Actor.remote()
    ray.get(a.check.remote())


def test_override_failure(shutdown_only):
    """Tests invalid override behaviors."""
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


@contextmanager
def chdir(d: str):
    old_dir = os.getcwd()
    os.chdir(d)
    yield
    os.chdir(old_dir)


@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
def test_inheritance(start_cluster):
    """Tests that child tasks/actors inherit URIs properly."""
    cluster, address = start_cluster
    with tempfile.TemporaryDirectory() as tmpdir, chdir(tmpdir):
        with open("hello", "w") as f:
            f.write("world")

        ray.init(address, runtime_env={"working_dir": "."})

        # Make sure we aren't reading the original file.
        os.unlink("hello")

        @ray.remote
        class Test:
            def f(self):
                print(f"os.getcwd() {os.getcwd()}")
                return open("hello").read()

        # Passing working_dir URI through directly should work.
        env1 = ray.get_runtime_context().runtime_env
        assert "working_dir" in env1
        t = Test.options(runtime_env=env1).remote()
        assert ray.get(t.f.remote()) == "world"

        # Passing a local directory should not work.
        env2 = ray.get_runtime_context().runtime_env
        env2["working_dir"] = "."
        with pytest.raises(ValueError):
            t = Test.options(runtime_env=env2).remote()


@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
def test_large_file_boundary(shutdown_only):
    """Check that packages just under the max size work as expected."""
    with tempfile.TemporaryDirectory() as tmp_dir, chdir(tmp_dir):
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


@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
def test_large_file_error(shutdown_only):
    with tempfile.TemporaryDirectory() as tmp_dir, chdir(tmp_dir):
        # Write to two separate files, each of which is below the threshold to
        # make sure the error is for the full package size.
        size = GCS_STORAGE_MAX_SIZE // 2 + 1
        with open("test_file_1", "wb") as f:
            f.write(os.urandom(size))

        with open("test_file_2", "wb") as f:
            f.write(os.urandom(size))

        with pytest.raises(RuntimeError):
            ray.init(runtime_env={"working_dir": "."})


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
