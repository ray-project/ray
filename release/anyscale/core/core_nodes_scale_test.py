import ray
import math
import time
import random
import argparse
import ray._private.test_utils as test_utils


NUM_SAMPLING_TASKS = 100000
NUM_SAMPLES_PER_TASK = 1000000
TOTAL_NUM_SAMPLES = NUM_SAMPLING_TASKS * NUM_SAMPLES_PER_TASK


@ray.remote(num_cpus=1)
class ProgressActor:
    def __init__(self, total_num_samples: int):
        self.total_num_samples = total_num_samples
        self.num_samples_completed_per_task = {}

    def report_progress(self, task_id: int, num_samples_completed: int) -> None:
        self.num_samples_completed_per_task[task_id] = num_samples_completed

    def get_progress(self) -> float:
        return (
            sum(self.num_samples_completed_per_task.values()) / self.total_num_samples
        )


@ray.remote(scheduling_strategy="SPREAD", num_cpus=1)
def sampling_task(
    num_samples: int, task_id: int, progress_actor: ray.actor.ActorHandle
) -> int:
    num_inside = 0
    for i in range(num_samples):
        x, y = random.uniform(-1, 1), random.uniform(-1, 1)
        if math.hypot(x, y) <= 1:
            num_inside += 1

        # Report progress and sleep a bit (to see if autoscaler is able to scale up)
        if (i + 1) % 5_000 == 0:
            # This is async.
            progress_actor.report_progress.remote(task_id, i + 1)
            time.sleep(3)

    # Report the final progress.
    progress_actor.report_progress.remote(task_id, num_samples)
    return num_inside


def parse_script_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--target_num_nodes", type=int, default=2000)
    parser.add_argument("--time_to_run_after_target_num_nodes", type=int, default=300)
    return parser.parse_known_args()


def send_to_output_file(target_num_nodes, final_node_count, time_to_scale_up_to_target):
    # write results to output file
    results = {
        "final_node_count": final_node_count,
        "time_to_scale_up": time_to_scale_up_to_target,
        "success": "1",
    }
    results["perf_metrics"] = [
        {
            "perf_metric_name": f"time_to_scale_up_to_{target_num_nodes}_nodes",
            "perf_metric_value": time_to_scale_up_to_target,
            "perf_metric_type": "LATENCY",
        }
    ]
    test_utils.safe_write_to_results_json(results)


def main():
    args, _ = parse_script_args()
    target_num_nodes = args.target_num_nodes
    time_to_run_after_target_num_nodes = args.time_to_run_after_target_num_nodes
    ray.init()

    progress_actor = ProgressActor.remote(TOTAL_NUM_SAMPLES)

    _ = [
        sampling_task.remote(NUM_SAMPLES_PER_TASK, i, progress_actor)
        for i in range(NUM_SAMPLING_TASKS)
    ]

    reached_node_goal = False
    start_time = time.time()
    stop_time = 0
    while True:
        progress = ray.get(progress_actor.get_progress.remote())
        num_alive_nodes = len([node for node in ray.nodes() if node["Alive"]])
        print(
            f"Number of Nodes: {num_alive_nodes}"
            f" time: {time.time() - start_time} progress: {progress}"
        )

        if reached_node_goal and num_alive_nodes < target_num_nodes:
            print(
                f"Went back UNDER {target_num_nodes} after reaching node goal, "
                f"will restart timer when node goal is reached again."
            )
            reached_node_goal = False

        if not reached_node_goal and (num_alive_nodes >= target_num_nodes):
            print(
                f"OVER {target_num_nodes},"
                f" Stopping in {time_to_run_after_target_num_nodes}."
            )
            reached_node_goal = True
            stop_time = time.time()

        if (
            reached_node_goal
            and time.time() - stop_time >= time_to_run_after_target_num_nodes
        ):
            final_node_count = num_alive_nodes
            break

        if progress == 1:
            # With current configurations this should never happen.
            print(f"Finished task before hitting {target_num_nodes} nodes")
            raise Exception(
                f"Failed to scale up to {target_num_nodes} nodes"
                f" before finishing task, got up to {num_alive_nodes} nodes"
            )

        time.sleep(10)

    send_to_output_file(target_num_nodes, final_node_count, stop_time - start_time)


if __name__ == "__main__":
    main()
