import argparse
import ray

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Dashboard agent.")
    parser.add_argument(
        "--conflict",
        type=str,
    )
    args = parser.parse_args()

    if args.conflict == "conda":
        ray.init(runtime_env={"conda": ["requests"]})
        print(ray._private.worker.global_worker.runtime_env)
    elif args.conflict == "env_vars":
        ray.init(runtime_env={"env_vars": {"A": "1"}})
        print(ray._private.worker.global_worker.runtime_env)
    else:
        ray.init(runtime_env={
            "env_vars": {"C": "1"},
            "worker_process_setup_hook": lambda: print("a"),
        })
        print(ray._private.worker.global_worker.runtime_env)
