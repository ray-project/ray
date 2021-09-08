import asyncio
from dataclasses import dataclass
from enum import Enum
import ray
from ray.types import ObjectRef
from ray.experimental.workflow.common import calculate_identifier
from ray.experimental.workflow.storage import Storage
from ray.experimental.workflow import workflow_storage
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from ray.actor import ActorHandle

MANAGEMENT_ACTOR_NAME = "StorageManagementActor"
MANAGEMENT_ACTOR_NAMESPACE = "workflow"


def init_manager(storage: Storage) -> None:
    handle = Manager.options(
        name=MANAGEMENT_ACTOR_NAME,
        namespace=MANAGEMENT_ACTOR_NAMESPACE,
        lifetime="detached").remote(storage)
    ray.get(handle.ping.remote())


def get_manager() -> "ActorHandle":
    return ray.get_actor(
        MANAGEMENT_ACTOR_NAME, namespace=MANAGEMENT_ACTOR_NAMESPACE)


class UploadState(Enum):
    IN_PROGRESS = 1
    FINISHED = 2


@dataclass
class Upload:
    state: UploadState
    identifier_ref: ObjectRef[str]
    upload_task: Optional[ray.ObjectRef]


@ray.remote
def _put_helper(identifier: str, obj: Any,
                wf_storage: "workflow_storage.WorkflowStorage") -> None:
    paths = wf_storage._key_obj_id(identifier)
    asyncio.get_event_loop().run_until_complete(wf_storage._put(paths, obj))
    return None


@ray.remote(num_cpus=0)
class Manager:
    """
    Responsible for deduping the serialization/upload of object references.
    """

    def __init__(self, storage: Storage):
        self.uploads: Dict[ray.ObjectRef, Upload] = {}
        self.storage = storage

    def ping(self) -> None:
        """
        Trivial function to ensure actor creation is successful.
        """
        return None

    async def save_objectref(
            self, ref_tuple: Tuple[ray.ObjectRef],
            workflow_id: "str") -> Tuple[List[str], ray.ObjectRef]:
        """
        Serialize and upload an object reference exactly once.

        Args:
            ref_tuple: A 1-element tuple which wraps the reference.
        """
        wf_storage = workflow_storage.WorkflowStorage(workflow_id,
                                                      self.storage)
        ref, = ref_tuple
        # Use the hex as the key to avoid holding a reference to the object.
        key = ref.hex()

        if key not in self.uploads:
            identifier_ref = calculate_identifier.remote(ref)
            self.uploads[key] = Upload(UploadState.IN_PROGRESS, identifier_ref,
                                       None)
            # TODO(Alex): We should probably eventually remove this upload_task
            # so the ref can be freed.
            self.uploads[key].upload_task = \
                _put_helper.remote(identifier_ref, ref, wf_storage)

        info = self.uploads[key]
        identifer = await info.identifier_ref
        paths = wf_storage._key_obj_id(identifer)
        return paths, info.upload_task
