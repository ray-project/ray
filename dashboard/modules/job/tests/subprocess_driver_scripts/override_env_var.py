"""
Test script that attempts to set its own runtime_env, but we should ensure
we ended up using job submission API call's runtime_env instead of scripts
"""


def run():
    import ray
    import os

    ray.init(
        address=os.environ["RAY_ADDRESS"],
        runtime_env={
            "env_vars": {"TEST_SUBPROCESS_JOB_CONFIG_ENV_VAR": "SHOULD_BE_OVERRIDEN"}
        },
    )

    @ray.remote
    def foo():
        return "bar"

    ray.get(foo.remote())
    print(os.environ.get("TEST_SUBPROCESS_JOB_CONFIG_ENV_VAR", None))


if __name__ == "__main__":
    run()
