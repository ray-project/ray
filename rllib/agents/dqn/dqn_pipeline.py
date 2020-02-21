"""Experimental pipeline-based impl; run this with --run='DQN_pl'"""

from ray.rllib.agents.dqn.dqn import DQNTrainer
from ray.rllib.utils.experimental_dsl import (AsyncGradients, ApplyGradients,
                                              StandardMetricsReporting)


def training_pipeline(workers, config):
    local_replay_buffer = ReplayBuffer()
    rollouts = ParallelRollouts(workers)

    def update_target_if_needed(train_out):
        pass

    # (1) Save experiences into the local replay buffer.
    save_op = rollouts.for_each(SaveToReplayBuffer(local_replay_buffer))

    # (2) Replay and train on selected experiences.
    train_op = (
        ParallelReplay(local_replay_buffer) \
            .for_each(TrainOneStep(workers)) \
            .for_each(update_target_if_needed))

    # Alternate between (1) and (2). We set deterministic=True so that we
    # always execute one train step per replay buffer update.
    combined = InterleavedExecution([save_op, replay_op], deterministic=True)

    return StandardMetricsReporting(combined, workers, config)


DQNPipeline = DQNTrainer.with_updates(training_pipeline=training_pipeline)
