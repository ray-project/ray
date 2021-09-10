import asyncio
from dataclasses import dataclass
import logging
import ray
from ray.types import ObjectRef
from ray.experimental.workflow import common
from ray.experimental.workflow import storage
from ray.experimental.workflow import workflow_storage
from typing import Any, Dict, List, Tuple, TYPE_CHECKING

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
            common.STORAGE_ACTOR_NAME,
            namespace=common.MANAGEMENT_ACTOR_NAMESPACE)
    except ValueError:
        store = storage.get_global_storage()
        if warn_on_creation:
            logger.warning("Cannot access workflow serialization manager. It "
                           "could be because "
                           "the workflow manager exited unexpectedly. A new "
                           "workflow manager is being created with storage "
                           f"'{store}'.")
        handle = Manager.options(
            name=common.STORAGE_ACTOR_NAME,
            namespace=common.MANAGEMENT_ACTOR_NAMESPACE,
            lifetime="detached").remote(store)
        ray.get(handle.ping.remote())
        return handle


@dataclass
class Upload:
    identifier_ref: ObjectRef[str]
    upload_task: ObjectRef[None]


@ray.remote(num_cpus=0)
def _put_helper(identifier: str, obj: Any,
                wf_storage: "workflow_storage.WorkflowStorage") -> None:
    if isinstance(obj, ray.ObjectRef):
        raise NotImplementedError("Workflow does not support checkpointing "
                                  "nested object references yet.")
    paths = wf_storage._key_obj_id(identifier)
    asyncio.get_event_loop().run_until_complete(
        wf_storage._put(paths, obj, update=False))


@ray.remote(num_cpus=0)
class Manager:
    """
    Responsible for deduping the serialization/upload of object references.
    """

    def __init__(self, storage: storage.Storage):
        self._uploads: Dict[ray.ObjectRef, Upload] = {}
        self._storage = storage
        self._num_uploads = 0

    def ping(self) -> None:
        """
        Trivial function to ensure actor creation is successful.
        """
        return None

    async def save_objectref(
            self, ref_tuple: Tuple[ray.ObjectRef],
            workflow_id: "str") -> Tuple[List[str], ray.ObjectRef]:
        """Serialize and upload an object reference exactly once.

        Args:
            ref_tuple: A 1-element tuple which wraps the reference.

        Returns:
            A pair. The first element is the paths the ref will be uploaded to.
            The second is an object reference to the upload task.
        """
        wf_storage = workflow_storage.WorkflowStorage(workflow_id,
                                                      self._storage)
        ref, = ref_tuple
        print("SAVING OBJECT REF", ref)
        # Use the hex as the key to avoid holding a reference to the object.
        key = ref.hex()

        if key not in self._uploads:
            # TODO(Alex): We should probably eventually free these refs.
            identifier_ref = common.calculate_identifier.remote(ref)
            upload_task = _put_helper.remote(identifier_ref, ref, wf_storage)
            self._uploads[key] = Upload(identifier_ref, upload_task)
            self._num_uploads += 1

        info = self._uploads[key]
        identifer = await info.identifier_ref
        paths = wf_storage._key_obj_id(identifer)
        return paths, info.upload_task

    async def export_stats(self) -> Dict[str, Any]:
        return {"num_uploads": self._num_uploads}
