"""Runtime env test

TODO

Test owner: architkulkarni

Acceptance criteria: Should run through and print "PASSED."
"""

import ray
import random

ray.client().env({"pip": ["requests==2.26.0"]}).connect()

versions = ["2.21.0", "2.22.0", "2.23.0", "2.24.0", "2.25.0", "2.26.0"]
envs = [{
    "pip": [f"requests=={versions[i]}"]
} for i in range(len(versions) - 1)]
envs.append({})  # In an empty runtime env, we should have requests==2.26.0.

NUM_ITERATIONS = 1  # 100
NUM_CALLS_PER_ITERATION = 1  # 1000
NUM_CPUS = 12


@ray.remote
def check_version_task(expected_version: str):
    import requests
    assert requests.__version__ == expected_version


for i in range(NUM_ITERATIONS):
    results = []
    for i in range(12):
        env, expected_version = random.choice(zip(envs, versions))
        remote_task = check_version_task.options(runtime_env=env)
        results.extend([
            remote_task.remote(expected_version)
            for _ in range(NUM_CALLS_PER_ITERATION)
        ])
    ray.get(results)


@ray.remote
class TestActor:
    def check_version(expected_version: str):
        import requests
        assert requests.__version__ == expected_version

    def nested_check_version(expected_version: str):
        ray.get(check_version_task.remote(expected_version))


for i in range(NUM_ITERATIONS):
    results = []
    for i in range(12):
        env, expected_version = random.choice(zip(envs, versions))
        actor = TestActor.options(runtime_env=env).remote()
        results.extend([
            actor.check_version.remote(expected_version)
            for _ in range(NUM_CALLS_PER_ITERATION)
        ])
        results.extend([
            actor.nested_check_version.remote(expected_version)
            for _ in range(NUM_CALLS_PER_ITERATION)
        ])
    ray.get(results)
