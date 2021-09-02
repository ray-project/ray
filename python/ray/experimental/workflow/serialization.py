import cloudpickle
from ray.experimental.workflow.workflow_storage import WorkflowStorage
from typing import Any




def serialize(obj: Any, wf_storage: WorkflowStorage) -> bytes:
    """Serializes an arbitrary object, handling references.

    Args:
        obj: The object to serialize. If it contains object references, those
                will be serialized too.
        wf_storage: The workflow storage. If an object contains references,
            they will be serialized, `wf_storage.put` will be called on them,
            and `wf_storage.get` will be used to restore the objects when the
            return value is pickled.
    Returns:
        The serialized version of the object. `pickle.loads` can be used
        to restore the object. If the original object contained object
        references, those will be read from storage and properly restored.

    Side effects:
        Any object references will be uploaded to their global, remote storage.
    """
    # NOTE: Cloudpickle doesn't support private dispatch tables, so we extend
    # the cloudpickler instead to avoid changing cloudpickle's global dispatch
    # table which is shared with `ray.put`. See
    # https://github.com/cloudpipe/cloudpickle/issues/437
    class ObjectRefPickler(cloudpickle.CloudPickler):
        _object_ref_reducer = {
            ray.ObjectRef: lambda ref: self._reduce_objectref(
                ref, upload_tasks)
        }
        dispatch_table = ChainMap(
            _object_ref_reducer,
            cloudpickle.CloudPickler.dispatch_table)
        dispatch = dispatch_table

    pickler = ObjectRefPickler(output_buffer)
    pickler.dump(data)
    output_buffer.seek(0)
    value = output_buffer.read()




