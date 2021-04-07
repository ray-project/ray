from typing import List, Any, Optional
import random

from ray.util.iter import from_actors, LocalIterator, _NextValueNotReady
from ray.util.iter_metrics import SharedMetrics
from ray.rllib.execution.replay_buffer import LocalReplayBuffer, \
    warn_replay_buffer_size
from ray.rllib.execution.common import \
    STEPS_SAMPLED_COUNTER, _get_shared_metrics
from ray.rllib.utils.typing import SampleBatchType


class StoreToReplayBuffer:
    """Callable that stores data into replay buffer actors.

    If constructed with a local replay actor, data will be stored into that
    buffer. If constructed with a list of replay actor handles, data will
    be stored randomly among those actors.

    This should be used with the .for_each() operator on a rollouts iterator.
    The batch that was stored is returned.

    Examples:
        >>> actors = [ReplayActor.remote() for _ in range(4)]
        >>> rollouts = ParallelRollouts(...)
        >>> store_op = rollouts.for_each(StoreToReplayActors(actors=actors))
        >>> next(store_op)
        SampleBatch(...)
    """

    def __init__(self,
                 *,
                 local_buffer: LocalReplayBuffer = None,
                 actors: List["ActorHandle"] = None):
        if bool(local_buffer) == bool(actors):
            raise ValueError(
                "Exactly one of local_buffer and replay_actors must be given.")

        if local_buffer:
            self.local_actor = local_buffer
            self.replay_actors = None
        else:
            self.local_actor = None
            self.replay_actors = actors

    def __call__(self, batch: SampleBatchType):
        if self.local_actor:
            self.local_actor.add_batch(batch)
        else:
            actor = random.choice(self.replay_actors)
            actor.add_batch.remote(batch)
        return batch


def Replay(*,
           local_buffer: LocalReplayBuffer = None,
           actors: List["ActorHandle"] = None,
           num_async: int = 4) -> LocalIterator[SampleBatchType]:
    """Replay experiences from the given buffer or actors.

    This should be combined with the StoreToReplayActors operation using the
    Concurrently() operator.

    Args:
        local_buffer (LocalReplayBuffer): Local buffer to use. Only one of this
            and replay_actors can be specified.
        actors (list): List of replay actors. Only one of this and
            local_buffer can be specified.
        num_async (int): In async mode, the max number of async
            requests in flight per actor.

    Examples:
        >>> actors = [ReplayActor.remote() for _ in range(4)]
        >>> replay_op = Replay(actors=actors)
        >>> next(replay_op)
        SampleBatch(...)
    """

    if bool(local_buffer) == bool(actors):
        raise ValueError(
            "Exactly one of local_buffer and replay_actors must be given.")

    if actors:
        replay = from_actors(actors)
        return replay.gather_async(
            num_async=num_async).filter(lambda x: x is not None)

    def gen_replay(_):
        while True:
            item = local_buffer.replay()
            if item is None:
                yield _NextValueNotReady()
            else:
                yield item

    return LocalIterator(gen_replay, SharedMetrics())


class WaitUntilTimestepsElapsed:
    """Callable that returns True once a given number of timesteps are hit."""

    def __init__(self, target_num_timesteps: int):
        self.target_num_timesteps = target_num_timesteps

    def __call__(self, item: Any) -> bool:
        metrics = _get_shared_metrics()
        ts = metrics.counters[STEPS_SAMPLED_COUNTER]
        return ts > self.target_num_timesteps


# TODO(ekl) deprecate this in favor of the replay_sequence_length option.
class SimpleReplayBuffer:
    """Simple replay buffer that operates over batches."""

    def __init__(self,
                 num_slots: int,
                 replay_proportion: Optional[float] = None):
        """Initialize SimpleReplayBuffer.

        Args:
            num_slots (int): Number of batches to store in total.
        """
        self.num_slots = num_slots
        self.replay_batches = []
        self.replay_index = 0

    def add_batch(self, sample_batch: SampleBatchType) -> None:
        warn_replay_buffer_size(item=sample_batch, num_items=self.num_slots)
        if self.num_slots > 0:
            if len(self.replay_batches) < self.num_slots:
                self.replay_batches.append(sample_batch)
            else:
                self.replay_batches[self.replay_index] = sample_batch
                self.replay_index += 1
                self.replay_index %= self.num_slots

    def replay(self) -> SampleBatchType:
        return random.choice(self.replay_batches)


class MixInReplay:
    """This operator adds replay to a stream of experiences.

    It takes input batches, and returns a list of batches that include replayed
    data as well. The number of replayed batches is determined by the
    configured replay proportion. The max age of a batch is determined by the
    number of replay slots.
    """

    def __init__(self, num_slots: int, replay_proportion: float):
        """Initialize MixInReplay.

        Args:
            num_slots (int): Number of batches to store in total.
            replay_proportion (float): The input batch will be returned
                and an additional number of batches proportional to this value
                will be added as well.

        Examples:
            # replay proportion 2:1
            >>> replay_op = MixInReplay(rollouts, 100, replay_proportion=2)
            >>> print(next(replay_op))
            [SampleBatch(<input>), SampleBatch(<replay>), SampleBatch(<rep.>)]

            # replay proportion 0:1, replay disabled
            >>> replay_op = MixInReplay(rollouts, 100, replay_proportion=0)
            >>> print(next(replay_op))
            [SampleBatch(<input>)]
        """
        if replay_proportion > 0 and num_slots == 0:
            raise ValueError(
                "You must set num_slots > 0 if replay_proportion > 0.")
        self.replay_buffer = SimpleReplayBuffer(num_slots)
        self.replay_proportion = replay_proportion

    def __call__(self, sample_batch: SampleBatchType) -> List[SampleBatchType]:
        # Put in replay buffer if enabled.
        self.replay_buffer.add_batch(sample_batch)

        # Proportional replay.
        output_batches = [sample_batch]
        f = self.replay_proportion
        while random.random() < f:
            f -= 1
            output_batches.append(self.replay_buffer.replay())
        return output_batches
