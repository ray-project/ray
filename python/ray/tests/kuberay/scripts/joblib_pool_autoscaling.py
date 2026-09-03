import argparse
import json
import os
import time

import ray
from ray.util.multiprocessing import Pool


def work(item, delay_s):
    time.sleep(delay_s)
    return {
        "item": item,
        "node_id": ray.get_runtime_context().get_node_id(),
        "pid": os.getpid(),
    }


def wait_for_cluster_cpus(expected, timeout_s=180):
    deadline = time.monotonic() + timeout_s
    resources = None
    while time.monotonic() < deadline:
        resources = ray.cluster_resources()
        if resources.get("CPU", 0) == expected:
            return
        time.sleep(1)
    raise TimeoutError(
        f"Cluster CPU resources did not converge to {expected}: {resources}"
    )


def run_pool(task_count, delay_s):
    pool = Pool(
        min_size=0,
        max_size=2,
        idle_timeout_s=2,
        ray_remote_args={"num_cpus": 1},
    )
    try:
        results = [
            pool.apply_async(work, (item, delay_s)) for item in range(task_count)
        ]
        outputs = [result.get(timeout=180) for result in results]
        # Keep the Pool alive while actor retirement makes the cluster idle and
        # KubeRay removes both workers. This distinguishes idle scale-down from
        # scale-down caused by closing the Pool.
        wait_for_cluster_cpus(0)
    except BaseException:
        pool.terminate()
        raise
    else:
        pool.close()
    finally:
        pool.join()
    return outputs


def run_joblib(task_count, delay_s):
    import joblib

    from ray.util.joblib import register_ray

    register_ray()
    with joblib.parallel_backend(
        "ray",
        n_jobs=2,
        min_size=0,
        max_size=2,
        idle_timeout_s=2,
        ray_remote_args={"num_cpus": 1},
    ):
        with joblib.Parallel(batch_size=1, pre_dispatch="all") as parallel:
            outputs = parallel(
                joblib.delayed(work)(item, delay_s) for item in range(task_count)
            )
            # Keep Joblib's backend (and its Pool) alive while proving that idle
            # scale-down does not depend on backend teardown.
            wait_for_cluster_cpus(0)
            return outputs


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("pool", "joblib"))
    parser.add_argument("--tasks", type=int, default=4)
    parser.add_argument("--delay-s", type=float, default=10)
    args = parser.parse_args()

    ray.init(address="auto")
    wait_for_cluster_cpus(0)

    started = time.monotonic()
    if args.mode == "pool":
        outputs = run_pool(args.tasks, args.delay_s)
    else:
        outputs = run_joblib(args.tasks, args.delay_s)

    node_ids = {output["node_id"] for output in outputs}
    if len(node_ids) != 2:
        raise AssertionError(f"Expected work on two worker nodes: {outputs}")
    print(
        "JOBLIB_AUTOSCALING_E2E="
        + json.dumps(
            {
                "mode": args.mode,
                "task_count": args.tasks,
                "worker_node_count": len(node_ids),
                "elapsed_s": round(time.monotonic() - started, 3),
                "outputs": outputs,
            },
            sort_keys=True,
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
