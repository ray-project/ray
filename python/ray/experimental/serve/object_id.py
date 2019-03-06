"""
Helper methods for dealing with ray.ObjectID
"""

import ray


def unwrap(future):
    return ray.get(future)[0]


def get_new_oid():
    worker = ray.worker.global_worker
    oid = ray._raylet.compute_put_id(
        worker.current_task_id, worker.task_context.put_index
    )
    worker.task_context.put_index += 1
    return oid
