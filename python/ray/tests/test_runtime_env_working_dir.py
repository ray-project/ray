from importlib import import_module
import os
from pathlib import Path
import sys
import tempfile
import shutil

import pytest

import ray
import time

# This test requires you have AWS credentials set up (any AWS credentials will
# do, this test only accesses a public bucket).

# This package contains a subdirectory called `test_module`.
# Calling `test_module.one()` should return `2`.
# If you find that confusing, take it up with @jiaodong...
HTTPS_PACKAGE_URI = ("https://github.com/shrekris-anyscale/"
                     "test_module/archive/HEAD.zip")
S3_PACKAGE_URI = "s3://runtime-env-test/test_runtime_env.zip"
GS_PACKAGE_URI = "gs://public-runtime-env-test/test_module.zip"


@pytest.mark.parametrize(
    "option", ["failure", "working_dir", "working_dir_zip", "py_modules"])
@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
def test_lazy_reads(start_cluster, tmp_working_dir, option: str):
    """Tests the case where we lazily read files or import inside a task/actor.

    This tests both that this fails *without* the working_dir and that it
    passes with it.
    """
    cluster, address = start_cluster

    def call_ray_init():
        if option == "failure":
            # Don't pass the files at all, so it should fail!
            ray.init(address)
        elif option == "working_dir":
            ray.init(address, runtime_env={"working_dir": tmp_working_dir})
        elif option == "working_dir_zip":
            # Create a temp dir to place the zipped package
            # from tmp_working_dir
            with tempfile.TemporaryDirectory() as tmp_dir:
                zip_dir = Path(tmp_working_dir)
                package = shutil.make_archive(
                    os.path.join(tmp_dir, "test"), "zip", zip_dir)
                ray.init(address, runtime_env={"working_dir": package})
        elif option == "py_modules":
            ray.init(
                address,
                runtime_env={
                    "py_modules": [str(Path(tmp_working_dir) / "test_module")]
                })
        elif option == "py_modules_path":
            ray.init(
                address,
                runtime_env={
                    "py_modules": [Path(tmp_working_dir) / "test_module"]
                })

    call_ray_init()

    def reinit():
        ray.shutdown()
        # TODO(SongGuyang): Currently, reinit the driver will generate the same
        # job id. And if we reinit immediately after shutdown, raylet may
        # process new job started before old job finished in some cases. This
        # inconsistency could disorder the URI reference and delete a valid
        # runtime env. We sleep here to walk around this issue.
        time.sleep(5)
        call_ray_init()

    @ray.remote
    def test_import():
        import test_module
        return test_module.one()

    if option == "failure":
        with pytest.raises(ImportError):
            ray.get(test_import.remote())
    else:
        assert ray.get(test_import.remote()) == 1

    reinit()

    @ray.remote
    def test_read():
        return open("hello").read()

    if option == "failure":
        with pytest.raises(FileNotFoundError):
            ray.get(test_read.remote())
    elif option == "working_dir":
        assert ray.get(test_read.remote()) == "world"

    reinit()

    @ray.remote
    class Actor:
        def test_import(self):
            import test_module
            return test_module.one()

        def test_read(self):
            return open("hello").read()

    a = Actor.remote()
    if option == "failure":
        with pytest.raises(ImportError):
            assert ray.get(a.test_import.remote()) == 1
        with pytest.raises(FileNotFoundError):
            assert ray.get(a.test_read.remote()) == "world"
    elif option == "working_dir":
        assert ray.get(a.test_import.remote()) == 1
        assert ray.get(a.test_read.remote()) == "world"


@pytest.mark.parametrize("option", ["failure", "working_dir", "py_modules"])
@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
def test_captured_import(start_cluster, tmp_working_dir, option: str):
    """Tests importing a module in the driver and capturing it in a task/actor.

    This tests both that this fails *without* the working_dir and that it
    passes with it.
    """
    cluster, address = start_cluster

    def call_ray_init():
        if option == "failure":
            # Don't pass the files at all, so it should fail!
            ray.init(address)
        elif option == "working_dir":
            ray.init(address, runtime_env={"working_dir": tmp_working_dir})
        elif option == "py_modules":
            ray.init(
                address,
                runtime_env={
                    "py_modules": [
                        os.path.join(tmp_working_dir, "test_module")
                    ]
                })

    call_ray_init()

    def reinit():
        ray.shutdown()
        # TODO(SongGuyang): Currently, reinit the driver will generate the same
        # job id. And if we reinit immediately after shutdown, raylet may
        # process new job started before old job finished in some cases. This
        # inconsistency could disorder the URI reference and delete a valid
        # runtime env. We sleep here to walk around this issue.
        time.sleep(5)
        call_ray_init()

    # Import in the driver.
    sys.path.insert(0, tmp_working_dir)
    import test_module

    @ray.remote
    def test_import():
        return test_module.one()

    if option == "failure":
        with pytest.raises(Exception):
            ray.get(test_import.remote())
    else:
        assert ray.get(test_import.remote()) == 1

    reinit()

    @ray.remote
    class Actor:
        def test_import(self):
            return test_module.one()

    if option == "failure":
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

        # Test that we can reconnect with no errors
        ray.shutdown()
        ray.init(address, runtime_env={"working_dir": working_dir})


@pytest.mark.parametrize("option", ["working_dir", "py_modules"])
@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
def test_input_validation(start_cluster, option: str):
    """Tests input validation for working_dir and py_modules."""
    cluster, address = start_cluster

    with pytest.raises(TypeError):
        if option == "working_dir":
            ray.init(address, runtime_env={"working_dir": 10})
        else:
            ray.init(address, runtime_env={"py_modules": [10]})

    ray.shutdown()

    with pytest.raises(ValueError):
        if option == "working_dir":
            ray.init(address, runtime_env={"working_dir": "/does/not/exist"})
        else:
            ray.init(address, runtime_env={"py_modules": ["/does/not/exist"]})

    ray.shutdown()

    with pytest.raises(ValueError):
        if option == "working_dir":
            ray.init(address, runtime_env={"working_dir": "does_not_exist"})
        else:
            ray.init(address, runtime_env={"py_modules": ["does_not_exist"]})

    ray.shutdown()

    for uri in ["https://no_dot_zip", "s3://no_dot_zip", "gs://no_dot_zip"]:
        with pytest.raises(ValueError):
            if option == "working_dir":
                ray.init(address, runtime_env={"working_dir": uri})
            else:
                ray.init(address, runtime_env={"py_modules": [uri]})

        ray.shutdown()

    if option == "py_modules":
        with pytest.raises(TypeError):
            # Must be in a list.
            ray.init(address, runtime_env={"py_modules": "."})


@pytest.mark.parametrize("option", ["working_dir", "py_modules"])
@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
def test_exclusion(start_cluster, tmp_working_dir, option):
    """Tests various forms of the 'excludes' parameter."""
    cluster, address = start_cluster

    def create_file(p, empty=False):
        if not p.parent.exists():
            p.parent.mkdir(parents=True)
        with p.open("w") as f:
            if not empty:
                f.write("Test")

    working_path = Path(tmp_working_dir)
    create_file(working_path / "__init__.py", empty=True)
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

    module_name = Path(tmp_working_dir).name

    # Test that all files are present without excluding.
    if option == "working_dir":
        ray.init(address, runtime_env={"working_dir": tmp_working_dir})
    else:
        ray.init(address, runtime_env={"py_modules": [tmp_working_dir]})

    @ray.remote
    def check_file(name):
        if option == "py_modules":
            try:
                module = import_module(module_name)
            except ImportError:
                return "FAILED"
            name = os.path.join(module.__path__[0], name)
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
    excludes = [
        # exclude by relative path
        "test2",
        # exclude by dir
        str(Path("tmp_dir") / "sub_dir"),
        # exclude part of the dir
        str(Path("tmp_dir") / "test_1"),
        # exclude part of the dir
        str(Path("tmp_dir") / "test_2"),
    ]

    if option == "working_dir":
        ray.init(
            address,
            runtime_env={
                "working_dir": tmp_working_dir,
                "excludes": excludes
            })
    else:
        ray.init(
            address,
            runtime_env={
                "py_modules": [tmp_working_dir],
                "excludes": excludes
            })

    assert get_all() == [
        "Test", "FAILED", "Test", "FAILED", "FAILED", "Test", "FAILED",
        "FAILED", "Test", "Test", "Test"
    ]

    ray.shutdown()

    # Test excluding all files using gitignore pattern matching syntax
    excludes = ["*"]
    if option == "working_dir":
        ray.init(
            address,
            runtime_env={
                "working_dir": tmp_working_dir,
                "excludes": excludes
            })
    else:
        module_name = Path(tmp_working_dir).name
        ray.init(
            address,
            runtime_env={
                "py_modules": [tmp_working_dir],
                "excludes": excludes
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

    if option == "working_dir":
        ray.init(address, runtime_env={"working_dir": tmp_working_dir})
    else:
        module_name = Path(tmp_working_dir).name
        ray.init(address, runtime_env={"py_modules": [tmp_working_dir]})

    assert get_all() == [
        "FAILED", "Test", "Test", "FAILED", "FAILED", "Test", "Test", "FAILED",
        "FAILED", "FAILED", "FAILED"
    ]


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


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
