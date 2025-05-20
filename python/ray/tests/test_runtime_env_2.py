import conda
import pytest
import time
import sys
import fnmatch
import os
from typing import List

import ray
from ray.dashboard.modules.job.common import JobStatus
from ray.exceptions import RuntimeEnvSetupError
from ray.runtime_env import RuntimeEnv, RuntimeEnvConfig
from ray._private.test_utils import wait_for_condition

if os.environ.get("RAY_MINIMAL") != "1":
    from ray.job_submission import JobSubmissionClient

bad_runtime_env_cache_ttl_seconds = 10


@pytest.mark.skipif(
    sys.version_info >= (3, 10, 0),
    reason=("Currently not passing on Python 3.10"),
)
@pytest.mark.parametrize("runtime_env_class", [dict, RuntimeEnv])
@pytest.mark.parametrize(
    "set_bad_runtime_env_cache_ttl_seconds",
    [
        str(bad_runtime_env_cache_ttl_seconds),
    ],
    indirect=True,
)
def test_invalid_conda_env(
    shutdown_only, runtime_env_class, set_bad_runtime_env_cache_ttl_seconds
):
    ray.init()

    @ray.remote
    def f():
        pass

    @ray.remote
    class A:
        def f(self):
            pass

    # TODO(somebody): track cache hit/miss statistics.
    conda_major_version = int(conda.__version__.split(".")[0])
    error_message = (
        "PackagesNotFoundError"
        if conda_major_version >= 24
        else "ResolvePackageNotFound"
    )

    bad_env = runtime_env_class(conda={"dependencies": ["this_doesnt_exist"]})
    with pytest.raises(
        RuntimeEnvSetupError,
        # The actual error message should be included in the exception.
        match=error_message,
    ):
        ray.get(f.options(runtime_env=bad_env).remote())

    # Check that another valid task can run.
    ray.get(f.remote())

    a = A.options(runtime_env=bad_env).remote()
    with pytest.raises(ray.exceptions.RuntimeEnvSetupError, match=error_message):
        ray.get(a.f.remote())

    with pytest.raises(RuntimeEnvSetupError, match=error_message):
        ray.get(f.options(runtime_env=bad_env).remote())

    # Sleep to wait bad runtime env cache removed.
    time.sleep(bad_runtime_env_cache_ttl_seconds)

    with pytest.raises(RuntimeEnvSetupError, match=error_message):
        ray.get(f.options(runtime_env=bad_env).remote())


def test_runtime_env_config(start_cluster):
    _, address = start_cluster
    bad_configs = []
    bad_configs.append({"setup_timeout_seconds": 10.0})
    bad_configs.append({"setup_timeout_seconds": 0})
    bad_configs.append({"setup_timeout_seconds": "10"})

    good_configs = []
    good_configs.append({"setup_timeout_seconds": 10})
    good_configs.append({"setup_timeout_seconds": -1})

    @ray.remote
    def f():
        return True

    def raise_exception_run(fun, *args, **kwargs):
        try:
            fun(*args, **kwargs)
        except Exception:
            pass
        else:
            assert False

    for bad_config in bad_configs:

        def run(runtime_env):
            raise_exception_run(ray.init, address, runtime_env=runtime_env)
            raise_exception_run(f.options, runtime_env=runtime_env)

        runtime_env = {"config": bad_config}
        run(runtime_env)

        raise_exception_run(RuntimeEnvConfig, **bad_config)
        raise_exception_run(RuntimeEnv, config=bad_config)

    for good_config in good_configs:

        def run(runtime_env):
            ray.shutdown()
            ray.init(address, runtime_env=runtime_env)
            assert ray.get(f.options(runtime_env=runtime_env).remote())

        runtime_env = {"config": good_config}
        run(runtime_env)
        runtime_env = {"config": RuntimeEnvConfig(**good_config)}
        run(runtime_env)
        runtime_env = RuntimeEnv(config=good_config)
        run(runtime_env)
        runtime_env = RuntimeEnv(config=RuntimeEnvConfig(**good_config))
        run(runtime_env)


def assert_no_user_info_in_logs(user_info: str, file_whitelist: List[str] = None):
    """Assert that the user info is not in the logs, except for any file that
    glob pattern matches a file in the whitelist.
    """
    if file_whitelist is None:
        file_whitelist = []

    log_dir = os.path.join(ray.worker._global_node.get_session_dir_path(), "logs")

    for root, dirs, files in os.walk(log_dir):
        for file in files:
            if any(fnmatch.fnmatch(file, pattern) for pattern in file_whitelist):
                continue
            # Some lines contain hex IDs, so ignore the UTF decoding errors.
            with open(os.path.join(root, file), "r", errors="ignore") as f:
                for line in f:
                    assert user_info not in line, (file, user_info, line)


class TestNoUserInfoInLogs:
    """Test that no user info (e.g. runtime env env vars) show up in the logs."""

    def test_assert_no_user_info_in_logs(self, shutdown_only):
        """Test assert_no_user_info_in_logs does not spuriously pass."""
        ray.init()
        with pytest.raises(AssertionError):
            assert_no_user_info_in_logs("ray")
        assert_no_user_info_in_logs("ray", file_whitelist=["*"])

    def test_basic(self, start_cluster, monkeypatch, tmp_path, shutdown_only):
        """Test driver with and without Ray Client."""

        cluster, address = start_cluster

        # Runtime env logs may still appear in debug logs. Check the debug flag is off.
        assert os.getenv("RAY_BACKEND_LOG_LEVEL") != "debug"

        # Reuse the same "secret" for working_dir, pip, env_vars for convenience.
        USER_SECRET = "pip-install-test"
        working_dir = tmp_path / USER_SECRET
        working_dir.mkdir()
        runtime_env = {
            "working_dir": str(working_dir),
            "pip": [USER_SECRET],
            # Append address to ensure different runtime envs for client and non-client
            # code paths to force reinstalling the runtime env instead of reusing it.
            "env_vars": {USER_SECRET: USER_SECRET + str(address)},
        }
        ray.init(runtime_env=runtime_env)

        # Run a function to ensure the runtime env is set up.
        @ray.remote
        def f():
            return os.environ.get(USER_SECRET)

        assert USER_SECRET in ray.get(f.remote())

        @ray.remote
        class Foo:
            def __init__(self):
                self.x = os.environ.get(USER_SECRET)

            def get_x(self):
                return self.x

        foo = Foo.remote()
        assert USER_SECRET in ray.get(foo.get_x.remote())

        # Generate runtime env failure logs too.
        bad_runtime_env = {
            "pip": ["pkg-which-sadly-does-not-exist"],
            "env_vars": {USER_SECRET: USER_SECRET},
        }
        with pytest.raises(Exception):
            ray.get(f.options(runtime_env=bad_runtime_env).remote())
        with pytest.raises(Exception):
            foo2 = Foo.options(runtime_env=bad_runtime_env).remote()
            ray.get(foo2.get_x.remote())

        using_ray_client = address.startswith("ray://")

        # Test Ray Jobs API codepath. Skip for ray_minimal because Ray Jobs API
        # requires ray[default]. Skip for Windows because Dashboard and Ray Jobs
        # are not tested on Windows.
        if (
            not using_ray_client
            and os.environ.get("RAY_MINIMAL") != "1"
            and not sys.platform == "win32"
        ):
            client = JobSubmissionClient()
            job_id_good_runtime_env = client.submit_job(
                entrypoint="echo 'hello world'", runtime_env=runtime_env
            )
            job_id_bad_runtime_env = client.submit_job(
                entrypoint="echo 'hello world'", runtime_env=bad_runtime_env
            )

            def job_succeeded(job_id):
                job_status = client.get_job_status(job_id)
                return job_status == JobStatus.SUCCEEDED

            def job_failed(job_id):
                job_status = client.get_job_status(job_id)
                return job_status == JobStatus.FAILED

            wait_for_condition(lambda: job_succeeded(job_id_good_runtime_env))
            wait_for_condition(lambda: job_failed(job_id_bad_runtime_env), timeout=30)

        with pytest.raises(AssertionError):
            assert_no_user_info_in_logs(USER_SECRET)

        assert_no_user_info_in_logs(
            USER_SECRET, file_whitelist=["runtime_env*.log", "event_EXPORT*.log"]
        )


if __name__ == "__main__":
    import os
    import sys

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
