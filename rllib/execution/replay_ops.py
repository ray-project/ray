

class StoreToReplayBuffer:
    """Callable that stores data into a local replay buffer.

    This should be used with the .for_each() operator on a rollouts iterator.
    The batch that was stored is returned.

    Examples:
        >>> buf = ReplayBuffer(1000)
        >>> rollouts = ParallelRollouts(...)
        >>> store_op = rollouts.for_each(StoreToReplayBuffer(buf))
        >>> next(store_op)
        SampleBatch(...)
    """

    def __init__(self, replay_buffer: ReplayBuffer):
        assert isinstance(replay_buffer, ReplayBuffer)
        self.replay_buffers = {DEFAULT_POLICY_ID: replay_buffer}

    def __call__(self, batch: SampleBatchType):
        # Handle everything as if multiagent
        if isinstance(batch, SampleBatch):
            batch = MultiAgentBatch({DEFAULT_POLICY_ID: batch}, batch.count)

        for policy_id, s in batch.policy_batches.items():
            for row in s.rows():
                self.replay_buffers[policy_id].add(
                    pack_if_needed(row["obs"]),
                    row["actions"],
                    row["rewards"],
                    pack_if_needed(row["new_obs"]),
                    row["dones"],
                    weight=None)
        return batch


class StoreToReplayActors:
    """Callable that stores data into a replay buffer actors.

    This should be used with the .for_each() operator on a rollouts iterator.
    The batch that was stored is returned.

    Examples:
        >>> actors = [ReplayActor.remote() for _ in range(4)]
        >>> rollouts = ParallelRollouts(...)
        >>> store_op = rollouts.for_each(StoreToReplayActors(actors))
        >>> next(store_op)
        SampleBatch(...)
    """

    def __init__(self, replay_actors: List["ActorHandle"]):
        self.replay_actors = replay_actors

    def __call__(self, batch: SampleBatchType):
        actor = random.choice(self.replay_actors)
        actor.add_batch.remote(batch)
        return batch


def ParallelReplay(replay_actors: List["ActorHandle"], async_queue_depth=4):
    """Replay experiences in parallel from the given actors.

    This should be combined with the StoreToReplayActors operation using the
    Concurrently() operator.

    Arguments:
        replay_actors (list): List of replay actors.
        async_queue_depth (int): In async mode, the max number of async
            requests in flight per actor.

    Examples:
        >>> actors = [ReplayActor.remote() for _ in range(4)]
        >>> replay_op = ParallelReplay(actors)
        >>> next(replay_op)
        SampleBatch(...)
    """
    replay = from_actors(replay_actors)
    return replay.gather_async(
        async_queue_depth=async_queue_depth).filter(lambda x: x is not None)


def LocalReplay(replay_buffer: ReplayBuffer, train_batch_size: int):
    """Replay experiences from a local buffer instance.

    This should be combined with the StoreToReplayBuffer operation using the
    Concurrently() operator.

    Arguments:
        replay_buffer (ReplayBuffer): Buffer to replay experiences from.
        train_batch_size (int): Batch size of fetches from the buffer.

    Examples:
        >>> actors = [ReplayActor.remote() for _ in range(4)]
        >>> replay_op = ParallelReplay(actors)
        >>> next(replay_op)
        SampleBatch(...)
    """
    assert isinstance(replay_buffer, ReplayBuffer)
    replay_buffers = {DEFAULT_POLICY_ID: replay_buffer}
    # TODO(ekl) support more options, or combine with ParallelReplay (?)
    synchronize_sampling = False
    prioritized_replay_beta = None

    def gen_replay(timeout):
        while True:
            samples = {}
            idxes = None
            for policy_id, replay_buffer in replay_buffers.items():
                if synchronize_sampling:
                    if idxes is None:
                        idxes = replay_buffer.sample_idxes(train_batch_size)
                else:
                    idxes = replay_buffer.sample_idxes(train_batch_size)

                if isinstance(replay_buffer, PrioritizedReplayBuffer):
                    metrics = LocalIterator.get_metrics()
                    num_steps_trained = metrics.counters[STEPS_TRAINED_COUNTER]
                    (obses_t, actions, rewards, obses_tp1, dones, weights,
                     batch_indexes) = replay_buffer.sample_with_idxes(
                         idxes,
                         beta=prioritized_replay_beta.value(num_steps_trained))
                else:
                    (obses_t, actions, rewards, obses_tp1,
                     dones) = replay_buffer.sample_with_idxes(idxes)
                    weights = np.ones_like(rewards)
                    batch_indexes = -np.ones_like(rewards)
                samples[policy_id] = SampleBatch({
                    "obs": obses_t,
                    "actions": actions,
                    "rewards": rewards,
                    "new_obs": obses_tp1,
                    "dones": dones,
                    "weights": weights,
                    "batch_indexes": batch_indexes
                })
            yield MultiAgentBatch(samples, train_batch_size)

    return LocalIterator(gen_replay, SharedMetrics())


