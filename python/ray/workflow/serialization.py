import contextlib
from dataclasses import dataclass
import logging
import os

import ray
from ray import cloudpickle
from ray.types import ObjectRef
from ray.workflow import common, workflow_storage
from typing import Any, Dict, Generator, List, Optional, Tuple, TYPE_CHECKING

from collections import ChainMap
import io

if TYPE_CHECKING:
    from ray.actor import ActorHandle

logger = logging.getLogger(__name__)


def init_manager() -> None:
    get_or_create_manager(warn_on_creation=False)


def get_or_create_manager(warn_on_creation: bool = True) -> "ActorHandle":
    """Get or create the storage manager."""
    # TODO(suquark): We should not get the actor everytime. We also need to
    # resume the actor if it failed. Using a global variable to cache the
    # actor seems not enough to resume the actor, because there is no
    # aliveness detection for an actor.
    try:
        return ray.get_actor(
            common.STORAGE_ACTOR_NAME, namespace=common.MANAGEMENT_ACTOR_NAMESPACE
        )
    except ValueError:
        if warn_on_creation:
            logger.warning(
                "Cannot access workflow serialization manager. It "
                "could be because "
                "the workflow manager exited unexpectedly. A new "
                "workflow manager is being created. "
            )
        handle = Manager.options(
            name=common.STORAGE_ACTOR_NAME,
            namespace=common.MANAGEMENT_ACTOR_NAMESPACE,
            lifetime="detached",
        ).remote()
        ray.get(handle.ping.remote())
        return handle


@dataclass
class Upload:
    identifier_ref: ObjectRef[str]
    upload_task: ObjectRef[None]


@ray.remote(num_cpus=0)
class Manager:
    """
    Responsible for deduping the serialization/upload of object references.
    """

    def __init__(self):
        self._uploads: Dict[ray.ObjectRef, Upload] = {}
        self._num_uploads = 0

    def ping(self) -> None:
        """
        Trivial function to ensure actor creation is successful.
        """
        return None

    async def save_objectref(
        self, ref_tuple: Tuple[ray.ObjectRef], workflow_id: "str"
    ) -> Tuple[List[str], ray.ObjectRef]:
        """Serialize and upload an object reference exactly once.

        Args:
            ref_tuple: A 1-element tuple which wraps the reference.

        Returns:
            A pair. The first element is the paths the ref will be uploaded to.
            The second is an object reference to the upload task.
        """
        (ref,) = ref_tuple
        # Use the hex as the key to avoid holding a reference to the object.
        key = (ref.hex(), workflow_id)

        if key not in self._uploads:
            # TODO(Alex): We should probably eventually free these refs.
            identifier_ref = common.calculate_identifier.remote(ref)
            upload_task = _put_helper.remote(identifier_ref, ref, workflow_id)
            self._uploads[key] = Upload(
                identifier_ref=identifier_ref, upload_task=upload_task
            )
            self._num_uploads += 1

        info = self._uploads[key]
        identifer = await info.identifier_ref
        key = _obj_id_to_key(identifer)
        return key, info.upload_task

    async def export_stats(self) -> Dict[str, Any]:
        return {"num_uploads": self._num_uploads}


OBJECTS_DIR = "objects"


def _obj_id_to_key(object_id: str) -> str:
    return os.path.join(OBJECTS_DIR, object_id)


@ray.remote(num_cpus=0)
def _put_helper(identifier: str, obj: Any, workflow_id: str) -> None:
    # TODO (Alex): This check isn't sufficient, it only works for directly
    # nested object refs.
    if isinstance(obj, ray.ObjectRef):
        raise NotImplementedError(
            "Workflow does not support checkpointing nested object references yet."
        )
    key = _obj_id_to_key(identifier)

    dump_to_storage(
        key,
        obj,
        workflow_id,
        workflow_storage.WorkflowStorage(workflow_id),
        update_existing=False,
    )


def _reduce_objectref(
    workflow_id: str,
    obj_ref: ObjectRef,
    tasks: List[ObjectRef],
):
    manager = get_or_create_manager()
    key, task = ray.get(manager.save_objectref.remote((obj_ref,), workflow_id))

    assert task
    tasks.append(task)

    return _load_object_ref, (key, workflow_id)


def dump_to_storage(
    key: str,
    obj: Any,
    workflow_id: str,
    storage: "workflow_storage.WorkflowStorage",
    update_existing=True,
) -> None:
    """Serializes and puts arbitrary object, handling references. The object will
        be uploaded at `paths`. Any object references will be uploaded to their
        global, remote storage.

    Args:
        key: The key of the object.
        obj: The object to serialize. If it contains object references, those
                will be serialized too.
        workflow_id: The workflow id.
        storage: The storage to use. If obj contains object references,
                `storage.put` will be called on them individually.
        update_existing: If False, the object will not be uploaded if the path
                exists.
    """
    if not update_existing:
        if storage._exists(key):
            return

    tasks = []

    # NOTE: Cloudpickle doesn't support private dispatch tables, so we extend
    # the cloudpickler instead to avoid changing cloudpickle's global dispatch
    # table which is shared with `ray.put`. See
    # https://github.com/cloudpipe/cloudpickle/issues/437
    class ObjectRefPickler(cloudpickle.CloudPickler):
        _object_ref_reducer = {
            ray.ObjectRef: lambda ref: _reduce_objectref(workflow_id, ref, tasks)
        }
        dispatch_table = ChainMap(
            _object_ref_reducer, cloudpickle.CloudPickler.dispatch_table
        )
        dispatch = dispatch_table

    ray.get(tasks)

    # TODO(Alex): We should be able to do this without the extra buffer.
    with io.BytesIO() as f:
        pickler = ObjectRefPickler(f)
        pickler.dump(obj)
        f.seek(0)
        # use the underlying storage to avoid cyclic calls of "dump_to_storage"
        storage._storage.put(key, f.read())


@ray.remote
def _load_ref_helper(key: str, workflow_id: str):
    # TODO(Alex): We should stream the data directly into `cloudpickle.load`.
    storage = workflow_storage.WorkflowStorage(workflow_id)
    return storage._get(key)


# TODO (Alex): We should use weakrefs here instead requiring a context manager.
_object_cache: Optional[Dict[str, ray.ObjectRef]] = None


def _load_object_ref(key: str, workflow_id: str) -> ray.ObjectRef:
    global _object_cache
    if _object_cache is None:
        return _load_ref_helper.remote(key, workflow_id)

    if _object_cache is None:
        return _load_ref_helper.remote(key, workflow_id)

    if key not in _object_cache:
        _object_cache[key] = _load_ref_helper.remote(key, workflow_id)

    return _object_cache[key]


@contextlib.contextmanager
def objectref_cache() -> Generator:
    """A reentrant caching context for object refs."""
    global _object_cache
    clear_cache = _object_cache is None
    if clear_cache:
        _object_cache = {}
    try:
        yield
    finally:
        if clear_cache:
            _object_cache = None
