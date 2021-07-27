"""Runtime env test

TODO

Test owner: architkulkarni

Acceptance criteria: Should run through and print "PASSED"
"""

import ray
import random
import os

if __name__ == "__main__":
    ray.init(address="auto", runtime_env={"pip": ["requests==2.26.0"]})
    versions = ["2.24.0", "2.25.0", "2.26.0"]
    envs = [{
        "pip": [f"requests=={versions[i]}"]
    } for i in range(len(versions) - 1)]
    # If a task's env is {}, we should have requests==2.26.0 from the job's env.
    envs.append({})  

    NUM_ITERATIONS = 100
    NUM_CALLS_PER_ITERATION = 1000
    NUM_ENVS_PER_ITERATION = 5

    if os.environ.get("IS_SMOKE_TEST"):
        NUM_ITERATIONS = 10
        NUM_CALLS_PER_ITERATION = 1
        NUM_ENVS_PER_ITERATION = 1
        
    print("Testing Tasks...")
    @ray.remote
    def check_version_task(expected_version: str):
        import requests
        assert requests.__version__ == expected_version


    for i in range(NUM_ITERATIONS):
        results = []
        for j in range(NUM_ENVS_PER_ITERATION):
            (env, expected_version) = random.choice(list(zip(envs, versions)))
            remote_task = check_version_task.options(runtime_env=env)
            results.extend([
                remote_task.remote(expected_version)
                for _ in range(NUM_CALLS_PER_ITERATION)
            ])
        ray.get(results)
        print(f"Finished iteration {i+1}/{NUM_ITERATIONS}")

    print("Testing Actors...")
    @ray.remote
    class TestActor:
        def check_version(self, expected_version: str):
            import requests
            assert requests.__version__ == expected_version

        def nested_check_version(self, expected_version: str):
            ray.get(check_version_task.remote(expected_version))


    for i in range(NUM_ITERATIONS):
        results = []
        for j in range(NUM_ENVS_PER_ITERATION):
            env, expected_version = random.choice(list(zip(envs, versions)))
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
        print(f"Finished iteration {i+1}/{NUM_ITERATIONS}")

    print("PASSED")
