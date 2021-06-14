import pathlib

import ray
import ray.cloudpickle

from ray.experimental.workflow import serialization_context


@ray.remote
def _save_to_checkpoints_task(checkpoints_dir, refs):
    with serialization_context.workflow_args_keeping_context():
        objs = ray.get(refs)
    # NOTE: we MUST NOT serialize the object inside the context.
    for ref, v in zip(refs, objs):
        path: pathlib.Path = checkpoints_dir / ref.hex()
        try:
            with open(path, "xb") as f:
                ray.cloudpickle.dump(v, f)
        except FileExistsError:
            pass


def checkpoint_refs(refs, checkpoints_dir, nonblocking=True):
    # TODO: avoid serializing the objects twice
    t = _save_to_checkpoints_task.remote(checkpoints_dir, refs)
    if not nonblocking:
        ray.get(t)
