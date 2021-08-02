"""Runtime env test with many tasks and actors

This test runs on four nodes and schedules many tasks and actors with
different runtime environments.

Test owner: architkulkarni

Acceptance criteria: Should run through and print "PASSED"
"""

import ray
import random
import os

if __name__ == "__main__":
    ray.init(address="auto", runtime_env={"pip": ["requests==2.18.0"]})
    versions = ["2.16.0", "2.17.0", "2.18.0"]
    envs = [{
        "pip": [f"requests=={versions[i]}"]
    } for i in range(len(versions) - 1)]
    # If a task's env is {}, we should have requests==2.18.0 from the job's env.
    envs.append({})

    NUM_TASK_ITERATIONS = 10
    NUM_ACTOR_ITERATIONS = 10
    NUM_CALLS_PER_ITERATION = 100
    NUM_ENVS_PER_ITERATION = 4

    if os.environ.get("IS_SMOKE_TEST") == "1":
        NUM_TASK_ITERATIONS = 10
        NUM_ACTOR_ITERATIONS = 10
        NUM_CALLS_PER_ITERATION = 1
        NUM_ENVS_PER_ITERATION = 1

    print("Testing Tasks...")
    @ray.remote
    def check_version_task(expected_version: str):
        import requests
        assert requests.__version__ == expected_version, (requests.__version__, expected_version)


    for i in range(NUM_TASK_ITERATIONS):
        results = []
        for j in range(NUM_ENVS_PER_ITERATION):
            (env, expected_version) = random.choice(list(zip(envs, versions)))
            remote_task = check_version_task.options(runtime_env=env)
            results.extend([
                remote_task.remote(expected_version)
                for _ in range(NUM_CALLS_PER_ITERATION)
            ])
        ray.get(results)
        print(f"Finished tasks iteration {i+1}/{NUM_TASK_ITERATIONS}")

    print("Testing Actors...")
    @ray.remote
    class TestActor:
        def check_version(self, expected_version: str):
            import requests
            assert requests.__version__ == expected_version, (requests.__version__, expected_version)

        def nested_check_version(self, expected_version: str):
            ray.get(check_version_task.remote(expected_version))


    for i in range(NUM_ACTOR_ITERATIONS):
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
        print(f"Finished actors iteration {i+1}/{NUM_ACTOR_ITERATIONS}")

    print("PASSED")
