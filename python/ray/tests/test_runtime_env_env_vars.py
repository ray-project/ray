# coding: utf-8
import os
import sys

import pytest

import ray


def test_environment_variables_task(ray_start_regular):
    @ray.remote
    def get_env(key):
        return os.environ.get(key)

    assert (
        ray.get(
            get_env.options(
                runtime_env={
                    "env_vars": {
                        "a": "b",
                    }
                }
            ).remote("a")
        )
        == "b"
    )


def test_environment_variables_actor(ray_start_regular):
    @ray.remote
    class EnvGetter:
        def get(self, key):
            return os.environ.get(key)

    a = EnvGetter.options(
        runtime_env={
            "env_vars": {
                "a": "b",
                "c": "d",
            }
        }
    ).remote()

    assert ray.get(a.get.remote("a")) == "b"
    assert ray.get(a.get.remote("c")) == "d"


def test_environment_variables_nested_task(ray_start_regular):
    @ray.remote
    def get_env(key):
        print(os.environ)
        return os.environ.get(key)

    @ray.remote
    def get_env_wrapper(key):
        return ray.get(get_env.remote(key))

    assert (
        ray.get(
            get_env_wrapper.options(
                runtime_env={
                    "env_vars": {
                        "a": "b",
                    }
                }
            ).remote("a")
        )
        == "b"
    )


def test_environment_variables_multitenancy(shutdown_only):
    ray.init(
        runtime_env={
            "env_vars": {
                "foo1": "bar1",
                "foo2": "bar2",
            }
        }
    )

    @ray.remote
    def get_env(key):
        return os.environ.get(key)

    assert ray.get(get_env.remote("foo1")) == "bar1"
    assert ray.get(get_env.remote("foo2")) == "bar2"
    assert (
        ray.get(
            get_env.options(
                runtime_env={
                    "env_vars": {
                        "foo1": "baz1",
                    }
                }
            ).remote("foo1")
        )
        == "baz1"
    )
    assert (
        ray.get(
            get_env.options(
                runtime_env={
                    "env_vars": {
                        "foo1": "baz1",
                    }
                }
            ).remote("foo2")
        )
        is None
    )


def test_environment_variables_complex(shutdown_only):
    ray.init(
        runtime_env={
            "env_vars": {
                "a": "job_a",
                "b": "job_b",
                "z": "job_z",
            }
        }
    )

    @ray.remote
    def get_env(key):
        return os.environ.get(key)

    @ray.remote
    class NestedEnvGetter:
        def get(self, key):
            return os.environ.get(key)

        def get_task(self, key):
            return ray.get(get_env.remote(key))

    @ray.remote
    class EnvGetter:
        def get(self, key):
            return os.environ.get(key)

        def get_task(self, key):
            return ray.get(get_env.remote(key))

        def nested_get(self, key):
            aa = NestedEnvGetter.options(
                runtime_env={
                    "env_vars": {
                        "c": "e",
                        "d": "dd",
                    }
                }
            ).remote()
            return ray.get(aa.get.remote(key))

    a = EnvGetter.options(
        runtime_env={
            "env_vars": {
                "a": "b",
                "c": "d",
            }
        }
    ).remote()

    assert ray.get(a.get.remote("a")) == "b"
    assert ray.get(a.get_task.remote("a")) == "b"
    assert ray.get(a.nested_get.remote("a")) is None
    assert ray.get(a.nested_get.remote("c")) == "e"
    assert ray.get(a.nested_get.remote("d")) == "dd"
    assert (
        ray.get(
            get_env.options(
                runtime_env={
                    "env_vars": {
                        "a": "b",
                    }
                }
            ).remote("a")
        )
        == "b"
    )

    assert ray.get(a.get.remote("z")) is None
    assert ray.get(a.get_task.remote("z")) is None
    assert ray.get(a.nested_get.remote("z")) is None
    assert (
        ray.get(
            get_env.options(
                runtime_env={
                    "env_vars": {
                        "a": "b",
                    }
                }
            ).remote("z")
        )
        is None
    )


def test_environment_variables_reuse(shutdown_only):
    """Test that new tasks don't incorrectly reuse previous environments."""
    ray.init()

    env_var_name = "TEST123"
    val1 = "VAL1"
    val2 = "VAL2"
    assert os.environ.get(env_var_name) is None

    @ray.remote
    def f():
        return os.environ.get(env_var_name)

    @ray.remote
    def g():
        return os.environ.get(env_var_name)

    assert ray.get(f.remote()) is None
    assert (
        ray.get(f.options(runtime_env={"env_vars": {env_var_name: val1}}).remote())
        == val1
    )
    assert ray.get(f.remote()) is None
    assert ray.get(g.remote()) is None
    assert (
        ray.get(f.options(runtime_env={"env_vars": {env_var_name: val2}}).remote())
        == val2
    )
    assert ray.get(g.remote()) is None
    assert ray.get(f.remote()) is None


# TODO(architkulkarni): Investigate flakiness on Travis CI.  It may be that
# there aren't enough CPUs (2-4 on Travis CI vs. likely 8 on Buildkite) and
# worker processes are being killed to adhere to the soft limit.
@pytest.mark.skipif(sys.platform == "darwin", reason="Flaky on Travis CI.")
def test_environment_variables_env_caching(shutdown_only):
    """Test that workers with specified envs are cached and reused.

    When a new task or actor is created with a new runtime env, a
    new worker process is started.  If a subsequent task or actor
    uses the same runtime env, the same worker process should be
    used.  This function checks the pid of the worker to test this.
    """
    ray.init()

    env_var_name = "TEST123"
    val1 = "VAL1"
    val2 = "VAL2"
    assert os.environ.get(env_var_name) is None

    def task():
        return os.environ.get(env_var_name), os.getpid()

    @ray.remote
    def f():
        return task()

    @ray.remote
    def g():
        return task()

    def get_options(val):
        return {"runtime_env": {"env_vars": {env_var_name: val}}}

    # Empty runtime env does not set our env var.
    assert ray.get(f.remote())[0] is None

    # Worker pid1 should have an env var set.
    ret_val1, pid1 = ray.get(f.options(**get_options(val1)).remote())
    assert ret_val1 == val1

    # Worker pid2 should have an env var set to something different.
    ret_val2, pid2 = ray.get(g.options(**get_options(val2)).remote())
    assert ret_val2 == val2

    # Because the runtime env is different, it should use a different process.
    assert pid1 != pid2

    # Call g with an empty runtime env. It shouldn't reuse pid2, because
    # pid2 has an env var set.
    _, pid3 = ray.get(g.remote())
    assert pid2 != pid3

    # Call g with the same runtime env as pid2. Check it uses the same process.
    _, pid4 = ray.get(g.options(**get_options(val2)).remote())
    assert pid4 == pid2

    # Call f with a different runtime env from pid1.  Check that it uses a new
    # process.
    _, pid5 = ray.get(f.options(**get_options(val2)).remote())
    assert pid5 != pid1

    # Call f with the same runtime env as pid1.  Check it uses the same
    # process.
    _, pid6 = ray.get(f.options(**get_options(val1)).remote())
    assert pid6 == pid1

    # Same as above but with g instead of f.  Shouldn't affect the outcome.
    _, pid7 = ray.get(g.options(**get_options(val1)).remote())
    assert pid7 == pid1


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
