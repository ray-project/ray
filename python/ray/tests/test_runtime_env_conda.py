import os
import pytest
import sys
from ray._private.test_utils import wait_for_condition
import yaml

import ray

if not os.environ.get("CI"):
    # This flags turns on the local development that link against current ray
    # packages and fall back all the dependencies to current python's site.
    os.environ["RAY_RUNTIME_ENV_LOCAL_DEV_MODE"] = "1"


def check_local_files_gced(cluster):
    for node in cluster.list_all_nodes():
        for subdir in ["conda"]:
            all_files = os.listdir(
                os.path.join(node.get_runtime_env_dir_path(), subdir))
            # Check that there are no files remaining except for .lock files
            # and generated requirements.txt files.
            # TODO(architkulkarni): these files should get cleaned up too!
            if len(
                    list(
                        filter(lambda f: not f.endswith((".lock", ".txt")),
                               all_files))) > 0:
                print(str(all_files))
                return False

    return True


def generate_runtime_env_dict(field, spec_format, tmp_path):
    if field == "conda":
        conda_dict = {
            "dependencies": ["pip", {
                "pip": ["pip-install-test==0.5"]
            }]
        }
        if spec_format == "file":
            conda_file = tmp_path / "environment.yml"
            conda_file.write_text(yaml.dump(conda_dict))
            conda = str(conda_file)
        elif spec_format == "python_object":
            conda = conda_dict
        runtime_env = {"conda": conda}
    elif field == "pip":
        if spec_format == "file":
            pip_file = tmp_path / "requirements.txt"
            pip_file.write_text("\n".join(["pip-install-test==0.5"]))
            pip = str(pip_file)
        elif spec_format == "python_object":
            pip = ["pip-install-test==0.5"]
        runtime_env = {"pip": pip}
    return runtime_env


@pytest.mark.skipif(
    os.environ.get("CI") and sys.platform != "linux",
    reason="Requires PR wheels built in CI, so only run on linux CI machines.")
@pytest.mark.parametrize("field", ["conda", "pip"])
@pytest.mark.parametrize("spec_format", ["file", "python_object"])
def test_job_level_gc(start_cluster, field, spec_format, tmp_path):
    """Tests that job-level conda env is GC'd when the job exits."""
    # We must use a single-node cluster.  If we simulate a multi-node cluster
    # then the conda installs will proceed simultaneously, one on each node,
    # but since they're actually running on the same machine we get errors.
    cluster, address = start_cluster

    ray.init(
        address,
        runtime_env=generate_runtime_env_dict(field, spec_format, tmp_path))

    @ray.remote
    def f():
        import pip_install_test  # noqa: F401
        return True

    # Ensure that the runtime env has been installed.
    assert ray.get(f.remote())

    assert not check_local_files_gced(cluster)

    ray.shutdown()

    wait_for_condition(lambda: check_local_files_gced(cluster), timeout=30)


@pytest.mark.skipif(
    os.environ.get("CI") and sys.platform != "linux",
    reason="Requires PR wheels built in CI, so only run on linux CI machines.")
@pytest.mark.parametrize("field", ["conda", "pip"])
@pytest.mark.parametrize("spec_format", ["file", "python_object"])
def test_detached_actor_gc(start_cluster, field, spec_format, tmp_path):
    """Tests that a detached actor's conda env is GC'd only when it exits."""
    cluster, address = start_cluster

    ray.init(
        address,
        namespace="test",
        runtime_env=generate_runtime_env_dict(field, spec_format, tmp_path))

    @ray.remote
    class A:
        def test_import(self):
            import pip_install_test  # noqa: F401
            return True

    a = A.options(name="test", lifetime="detached").remote()
    ray.get(a.test_import.remote())

    assert not check_local_files_gced(cluster)

    ray.shutdown()
    ray.init(address, namespace="test")

    assert not check_local_files_gced(cluster)

    a = ray.get_actor("test")
    assert ray.get(a.test_import.remote())

    ray.kill(a)

    wait_for_condition(lambda: check_local_files_gced(cluster), timeout=30)


# TODO(architkulkarni): fix bug #19602 and enable test.
@pytest.mark.skip("Currently failing")
@pytest.mark.skipif(
    os.environ.get("CI") and sys.platform != "linux",
    reason="Requires PR wheels built in CI, so only run on linux CI machines.")
@pytest.mark.parametrize("field", ["conda", "pip"])
@pytest.mark.parametrize("spec_format", ["file", "python_object"])
def test_actor_level_gc(start_cluster, field, spec_format, tmp_path):
    """Tests that actor-level working_dir is GC'd when the actor exits."""
    cluster, address = start_cluster

    ray.init(address)

    runtime_env = generate_runtime_env_dict(field, spec_format, tmp_path)

    @ray.remote
    class A:
        def test_import(self):
            import pip_install_test  # noqa: F401
            return True

    NUM_ACTORS = 5
    actors = [
        A.options(runtime_env=runtime_env).remote() for _ in range(NUM_ACTORS)
    ]
    ray.get([a.test_import.remote() for a in actors])
    for i in range(5):
        assert not check_local_files_gced(cluster)
        ray.kill(actors[i])
    wait_for_condition(lambda: check_local_files_gced(cluster))


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
