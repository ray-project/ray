from typing import List
import random

from ray.util.iter import from_actors, LocalIterator, _NextValueNotReady
from ray.util.iter_metrics import SharedMetrics
from ray.rllib.optimizers.async_replay_optimizer import LocalReplayBuffer
from ray.rllib.execution.common import SampleBatchType


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
           async_queue_depth=4):
    """Replay experiences from the given buffer or actors.

    This should be combined with the StoreToReplayActors operation using the
    Concurrently() operator.

    Arguments:
        local_buffer (LocalReplayBuffer): Local buffer to use. Only one of this
            and replay_actors can be specified.
        actors (list): List of replay actors. Only one of this and
            local_buffer can be specified.
        async_queue_depth (int): In async mode, the max number of async
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
        return replay.gather_async(async_queue_depth=async_queue_depth).filter(
            lambda x: x is not None)

    def gen_replay(_):
        while True:
            item = local_buffer.replay()
            if item is None:
                yield _NextValueNotReady()
            else:
                yield item

    return LocalIterator(gen_replay, SharedMetrics())
