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
           num_async=4):
    """Replay experiences from the given buffer or actors.

    This should be combined with the StoreToReplayActors operation using the
    Concurrently() operator.

    Arguments:
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


class MixInReplay:
    """This operator adds replay to a stream of experiences.

    It takes input batches, and returns a list of batches that include replayed
    data as well. The number of replayed batches is determined by the
    configured replay proportion. The max age of a batch is determined by the
    number of replay slots.
    """

    def __init__(self, num_slots, replay_proportion: float = None):
        """Initialize MixInReplay.

        Args:
            num_slots (int): Number of batches to store in total.
            replay_proportion (float): If None, one batch will be replayed per
                each input batch. Otherwise, the input batch will be returned
                and an additional number of batches proportional to this value
                will be added as well.

        Examples:
            # 1:1 mode (default)
            >>> replay_op = MixInReplay(rollouts, 100)
            >>> print(next(replay_op))
            SampleBatch(<replay>)

            # proportional mode
            >>> replay_op = MixInReplay(rollouts, 100, replay_proportion=2)
            >>> print(next(replay_op))
            [SampleBatch(<input>), SampleBatch(<replay>), SampleBatch(<rep.>)]

            # proportional mode, replay disabled
            >>> replay_op = MixInReplay(rollouts, 100, replay_proportion=0)
            >>> print(next(replay_op))
            [SampleBatch(<input>)]
        """
        if replay_proportion is not None:
            if replay_proportion > 0 and num_slots == 0:
                raise ValueError(
                    "You must set num_slots > 0 if replay_proportion > 0.")
        elif num_slots == 0:
            raise ValueError(
                "You must set num_slots > 0 if replay_proportion = None.")
        self.num_slots = num_slots
        self.replay_proportion = replay_proportion
        self.replay_batches = []
        self.replay_index = 0

    def __call__(self, sample_batch):
        # Put in replay buffer if enabled.
        if self.num_slots > 0:
            if len(self.replay_batches) < self.num_slots:
                self.replay_batches.append(sample_batch)
            else:
                self.replay_batches[self.replay_index] = sample_batch
                self.replay_index += 1
                self.replay_index %= self.num_slots

        # 1:1 replay mode.
        if self.replay_proportion is None:
            return random.choice(self.replay_batches)

        # Proportional replay mode.
        output_batches = [sample_batch]
        f = self.replay_proportion
        while random.random() < f:
            f -= 1
            replay_batch = random.choice(self.replay_batches)
            output_batches.append(replay_batch)
        return output_batches
