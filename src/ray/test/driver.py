import ray
from ray.utils import random_string

if __name__ == '__main__':
    import sys
    driver = ray.local_scheduler.LocalSchedulerClient(sys.argv[1],
                                                      random_string(),
                                                      random_string(), False,
                                                      0)

    task = ray.local_scheduler.Task(
        ray.local_scheduler.ObjectID(random_string()),
        ray.local_scheduler.ObjectID(random_string()),
        [],
        1,
        ray.local_scheduler.ObjectID(random_string()),
        0)
    print("submitting", task.task_id())
    driver.submit(task)

    print("Return values were", task.returns())
    task2 = ray.local_scheduler.Task(
        ray.local_scheduler.ObjectID(random_string()),
        ray.local_scheduler.ObjectID(random_string()),
        task.returns(),
        1,
        ray.local_scheduler.ObjectID(random_string()),
        0)
    print("submitting another task", task2.task_id())
    driver.submit(task2)
