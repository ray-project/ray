import argparse
import sys
import time

import ray

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Dashboard agent.")
    parser.add_argument(
        "--conflict",
        type=str,
    )
    parser.add_argument(
        "--worker-process-setup-hook",
        type=str,
    )

    args = parser.parse_args()

    if args.worker_process_setup_hook:
        ray.init(
            runtime_env={
                "worker_process_setup_hook": lambda: print(
                    args.worker_process_setup_hook
                )
            }
        )

        @ray.remote
        def f():
            pass

        ray.get(f.remote())
        time.sleep(5)
        sys.exit(0)

    if args.conflict == "pip":
        ray.init(runtime_env={"pip": ["numpy"]})
        print(ray._private.worker.global_worker.runtime_env)
    elif args.conflict == "env_vars":
        ray.init(runtime_env={"env_vars": {"A": "1"}})
        print(ray._private.worker.global_worker.runtime_env)
    else:
        ray.init(
            runtime_env={
                "env_vars": {"C": "1"},
            }
        )
        print(ray._private.worker.global_worker.runtime_env)
