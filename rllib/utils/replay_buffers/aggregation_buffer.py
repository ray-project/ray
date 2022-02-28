import platform

import ray
from ray.rllib.utils.replay_buffers.replay_buffer import ReplayBuffer


class AggregationBuffer(ReplayBuffer):
    """A ray.remote aggregation buffer shard plus mix-in replay functionality.

    RolloutWorkers can send their samples to such shard and it returns
    batches of size `train_batch_size`. This allows expensive decompression and
    concatenation work to be offloaded to these actors instead of running in the
    learner.
    """

    def __init__(self, capacity, storage_unit, *, mixin=True, **kwargs):
        super().__init__(capacity=capacity, storage_unit=storage_unit, **kwargs)

        self.mixin = mixin

    def add(self, batch: SampleBatchType, **kwargs) -> None:


            # Augment with replay and concat to desired train batch size.
            it = (
                it.zip_with_source_actor()
                .for_each(update_worker)
                .for_each(lambda batch: batch.decompress_if_needed())
                .for_each(
                    MixInReplay(
                        num_slots=config["replay_buffer_num_slots"],
                        replay_proportion=config["replay_proportion"],
                    )
                )
                .flatten()
                .combine(
                    ConcatBatches(
                        min_batch_size=config["train_batch_size"],
                        count_steps_by=config["multiagent"]["count_steps_by"],
                    )
                )
            )
