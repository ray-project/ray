import asyncio
import cloudpickle
from collections import ChainMap
import ray
from ray import ObjectRef
from ray.experimental.workflow.common import (
    calculate_identifiers)
from ray.experimental.workflow.workflow_storage import WorkflowStorage
from typing import Any, List


@ray.remote
def _put_helper(paths: List[str], obj: Any,
                wf_storage: WorkflowStorage) -> None:
    asyncio.get_event_loop().run_until_complete(
        wf_storage._put(paths, obj))

def _reduce_objectref(wf_storage: WorkflowStorage, obj_ref: ObjectRef,
                        tasks: List[ObjectRef]):

    # TODO (Alex): Ideally we could parallelize these hash calculations,
    # but we need the result before we can return from this reducer.
    identifier = calculate_identifiers([obj_ref]).pop()
    paths = wf_storage._key_obj_id(identifier)
    # TODO (Alex): We should dedupe these puts with the global coordinator.
    task = _put_helper.remote(paths, obj_ref, wf_storage)
    tasks.append(task)
    return _load_object_ref, (paths, wf_storage)


async def dump_to_storage(paths: List[str], obj: Any, wf_storage: WorkflowStorage) -> asyncio.Future:
    """
    Serializes and puts arbitrary object, handling references. The object will
        be uploaded at `paths`. Any object references will be uploaded to their
        global, remote storage.

    Args:
        paths: The location to put the object.
        obj: The object to serialize. If it contains object references, those
                will be serialized too.
        wf_storage: The workflow storage. If an object contains references,
            they will be serialized, `wf_storage.put` will be called on them,
            and `wf_storage.get` will be used to restore the objects when the
            return value is pickled.

    """
    tasks = []
    # NOTE: Cloudpickle doesn't support private dispatch tables, so we extend
    # the cloudpickler instead to avoid changing cloudpickle's global dispatch
    # table which is shared with `ray.put`. See
    # https://github.com/cloudpipe/cloudpickle/issues/437
    class ObjectRefPickler(cloudpickle.CloudPickler):
        _object_ref_reducer = {
            ray.ObjectRef: lambda ref: _reduce_objectref(
                wf_storage, ref, tasks)
        }
        dispatch_table = ChainMap(
            _object_ref_reducer,
            cloudpickle.CloudPickler.dispatch_table)
        dispatch = dispatch_table

    # TODO Expose the key related APIs.
    key = wf_storage._storage.make_key(*paths)
    with wf_storage.open(key, "w") as f:
        pickler = ObjectRefPickler(f)
        pickler.dump(obj)

    await asyncio.gather(*tasks)


@ray.remote
def _load_ref_helper(paths: List[str], wf_storage: WorkflowStorage):
    return asyncio.get_event_loop().run_until_complete(
        wf_storage._get(paths))


def _load_object_ref(paths: List[str],
                     wf_storage: WorkflowStorage) -> ObjectRef:

    return _load_ref_helper.remote(paths, wf_storage)


