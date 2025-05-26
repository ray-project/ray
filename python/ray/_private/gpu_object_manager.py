from typing import Dict, List, Optional, Tuple, TYPE_CHECKING
from ray.actor import ActorHandle
from ray.types import ObjectRef

if TYPE_CHECKING:
    import torch


class GPUObjectManager:
    def __init__(self):
        # A dictionary that maps from an object ID to a list of tensors.
        #
        # Note: Currently, `gpu_object_store` is only supported for Ray Actors.
        self.gpu_object_store: Dict[str, List["torch.Tensor"]] = {}
        # A dictionary that maps from an object ref to a tuple of (actor handle, object ref).
        # The key of the dictionary is an object ref that points to data consisting of tensors.
        # These tensors are stored in the GPU object store of the actor referenced by the ActorHandle.
        # The object ref in the tuple is the object ref of a list of tuples, each containing the shape
        # and dtype of a tensor.
        #
        # Note: The coordinator process (i.e., the driver process in most cases), which is responsible
        # for managing out-of-band tensor transfers between actors, uses `gpu_object_refs` to
        # determine whether `ObjectRef`s are stored in an actor's GPU object store or not.
        self.gpu_object_refs: Dict[ObjectRef, Tuple[ActorHandle, ObjectRef]] = {}

    def has_gpu_object(self, obj_id: str) -> bool:
        return obj_id in self.gpu_object_store

    def get_gpu_object(self, obj_id: str) -> Optional[List["torch.Tensor"]]:
        return self.gpu_object_store[obj_id]

    def add_gpu_object(self, obj_id: str, gpu_object: List["torch.Tensor"]):
        self.gpu_object_store[obj_id] = gpu_object

    def remove_gpu_object(self, obj_id: str):
        del self.gpu_object_store[obj_id]

    def add_gpu_object_ref(
        self, obj_ref: ObjectRef, actor_handle: ActorHandle, gpu_object_ref: ObjectRef
    ):
        self.gpu_object_refs[obj_ref] = (actor_handle, gpu_object_ref)

    def remove_gpu_object_ref(self, obj_ref: ObjectRef):
        del self.gpu_object_refs[obj_ref]

    def get_gpu_object_ref(
        self, obj_ref: ObjectRef
    ) -> Optional[Tuple[ActorHandle, ObjectRef]]:
        return self.gpu_object_refs[obj_ref]

    def is_gpu_object_ref(self, obj_ref: ObjectRef) -> bool:
        return obj_ref in self.gpu_object_refs
