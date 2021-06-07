import pathlib

import ray
import ray.cloudpickle

from ray.experimental.workflow import serialization_context


def save_object_to_file(obj, checkpoints_dir):
    assert isinstance(obj, ray.ObjectRef)
    path: pathlib.Path = checkpoints_dir / obj.hex()
    try:
        with open(path, "xb") as f:
            ray.cloudpickle.dump(ray.get(obj), f)
    except FileExistsError:
        pass


@ray.remote
def _save_to_checkpoints_task(checkpoints_dir, objs):
    with serialization_context.workflow_args_keeping_context():
        for v in objs:
            save_object_to_file(v, checkpoints_dir)


def checkpoint_refs(refs, checkpoints_dir, nonblocking=True):
    # TODO: avoid serializing the objects twice
    t = _save_to_checkpoints_task.remote(checkpoints_dir, refs)
    if not nonblocking:
        ray.get(t)
