import ray

from typing import Union, List, Dict, Any, Optional, Callable
from ray import ObjectRef
from ray.rllib.policy.sample_batch import SampleBatchType
from ray.rllib.utils.typing import T
from ray.rllib.utils.replay_buffers.replay_buffer import ReplayBuffer


# TODO: Implement Storage class after merging Storage class from Fwitter
#  class CheckpointedStorage(Storage): [...]


def _maybe_dereference(_object: Union[Any, ObjectRef], timeout) -> Any:
    if type(_object) is ObjectRef:
        return ray.get(_object, timeout=timeout)
    else:
        return _object


def get_distributed_buffer_class(base):
    class DistributedReplayBuffer(base):
        def add(self, batch: Union[SampleBatchType, ObjectRef], **kwargs) -> None:
            """Adds batch to the wrapped ReplayBuffer

            Adds it right away if batch is of type SampleBatchType. First gets the
            batch before adding it if batch is of type ObjectRef.

            Args:
                batch: The batch to be added or a reference thereof
                **kwargs: Forward compatibility kwargs or references thereof

            Returns:
                None
            """
            batch = _maybe_dereference(batch)
            for key, arg in kwargs.items():
                kwargs[key] = _maybe_dereference(arg)

            super().add(batch, **kwargs)

        def sample(
            self, num_items: int, as_object_ref: bool = False, **kwargs
        ) -> Union[SampleBatchType, ObjectRef]:
            """Calls the sample method of the underlying ReplayBuffer.

            Returns an ObjectRef to the sampled batch if `as_object_ref` is True.
            Otherwise, returns the sampled batch itself.

            Args:
                num_items: Number of items to sample
                as_object_ref: If True, returns an ObjectRef to the sampled batch.
                **kwargs: Forward compatibility kwargs or references thereof.

            Returns:
                The sampled batch, or references thereof.
            """
            for key, arg in kwargs.items():
                kwargs[key] = _maybe_dereference(arg)

            batch = super().sample(num_items, **kwargs)

            if as_object_ref:
                return ray.put(batch)
            else:
                return batch

    return DistributedReplayBuffer


class ReplayReplayBufferManager:
    def __init__(
        self,
        replay_buffers: list,
    ):
        self.replay_buffers = replay_buffers

    def add(self, batch: Union[SampleBatchType, ObjectRef], **kwargs) -> None:
        raise NotImplementedError
        # Chooses a remote buffer to add batch to and passes an ObjectRef to the
        # remote buffer

    def sample(
        self, num_items: int, as_object_ref: bool = False, **kwargs
    ) -> Union[SampleBatchType, ObjectRef]:
        raise NotImplementedError
        # Chooses a remote buffer to sample from and return the batch itself or an
        # ObjectRef to it

    def stats(self, debug: bool = False) -> List[dict]:
        raise NotImplementedError
        # Return concatenated List of stats dicts from all distributed buffers

    def get_state(self) -> List[Dict[str, Any]]:
        raise NotImplementedError
        # Return concatenated List of state dicts from all distributed buffers
        # The dicts include paths to checkpointed data

    def set_state(self, state: List[Dict[str, Any]]) -> None:
        raise NotImplementedError
        # Set states of all distributed buffers
        # The dicts may include paths to checkpointed data for the buffer to read from

    def apply(
        self,
        func: Callable[[ReplayBuffer, Optional[Any], Optional[Any]], T],
        *args,
        **kwargs,
    ) -> T:
        raise NotImplementedError
        # Applies a callable to all remote buffers
