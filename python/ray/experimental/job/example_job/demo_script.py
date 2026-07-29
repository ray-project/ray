# Regular ray application that user wrote and runs on local cluster.
# intermediate status are dumped to GCS
import argparse
import time

import ray
import ray.experimental.internal_kv as ray_kv


@ray.remote
class StepActor:
    def __init__(self, interval_s=1, total_steps=3):
        self.interval_s = interval_s
        self.stopped = False
        self.current_step = 1
        self.total_steps = total_steps

        worker = ray._private.worker.global_worker
        worker_id = worker.core_worker.get_actor_id()
        ray_kv._internal_kv_put(f"JOB:{worker_id}", self.current_step, overwrite=True)

    def run(self):
        worker = ray._private.worker.global_worker
        worker_id = worker.core_worker.get_actor_id()

        while self.current_step <= self.total_steps:
            if not self.stopped:
                print(
                    f"Sleeping {self.interval_s} secs to executing "
                    f"step {self.current_step}"
                )
                time.sleep(self.interval_s)
                self.current_step += 1
                ray_kv._internal_kv_put(
                    f"JOB:{worker_id}", self.current_step, overwrite=True
                )
            else:
                print("Stop called or reached final step.")
                break

        self.stopped = True
        ray_kv._internal_kv_put(f"JOB:{worker_id}", "DONE", overwrite=True)
        return "DONE"

    def get_step(self):
        return self.current_step

    def stop(self):
        self.stopped = True


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--interval-s",
        required=False,
        type=int,
        default=1,
        help="Address to use to connect to Ray",
    )
    parser.add_argument(
        "--total-steps",
        required=False,
        type=int,
        default=3,
        help="Password for connecting to Redis",
    )
    args, _ = parser.parse_known_args()

    ray.init()
    step_actor = StepActor.remote(
        interval_s=args.interval_s, total_steps=args.total_steps
    )
    ref = step_actor.run.remote()
    print(ray.get([ref]))
    job_key = ray_kv._internal_kv_list("JOB:")[0]
    print(f"{job_key}, {ray_kv._internal_kv_get(job_key)}")
