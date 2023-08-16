"""
A dummy ray driver script that executes in subprocess.
Prints global worker's `load_code_from_local` property that ought to be set whenever `JobConfig.code_search_path`
is specified
"""

import ray
from ray.job_config import JobConfig


def run():
    ray.init(job_config=JobConfig(code_search_path=["/home/code/"]))

    @ray.remote
    def foo() -> bool:
        return ray.global_worker.load_code_from_local

    load_code_from_local = ray.get(foo.remote())

    statement = "propagated" if load_code_from_local else "NOT propagated"

    # Step 1: Print the statement indicating that the code_search_path have been
    #         properly respected
    print(f"Code search path is {statement}")
    # Step 2: Print the whole runtime_env to validate that it's been passed appropriately
    #         from submit_job API
    print(ray.get_runtime_context().runtime_env)


if __name__ == "__main__":
    run()
