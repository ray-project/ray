"""Script to check ray job status:
- Start head node:
ray start --head --port=6379
- Start worker node:
ray start --address='172.31.11.87:6379'
- Check ray cluster status:
ray status
- Check job status:
ray job list
ray job status <job-id>
"""

import time

import ray

_NUM_TASKS = 5
_SLEEP_TIME_SEC = 100

ray.init("172.31.11.87:6379")


@ray.remote
def sleep_task():
    time.sleep(_SLEEP_TIME_SEC)
    return "Task Completed!"


tasks = [sleep_task.remote() for _ in range(_NUM_TASKS)]
results = ray.get(tasks)
print(results)
