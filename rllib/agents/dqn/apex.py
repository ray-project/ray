from ray.rllib.agents.dqn.dqn import DQNTrainer, DEFAULT_CONFIG as DQN_CONFIG
from ray.rllib.optimizers import AsyncReplayOptimizer
from ray.rllib.optimizers.replay_buffer import ReplayBuffer
from ray.rllib.optimizers.async_replay_optimizer import ReplayActor
from ray.rllib.utils import merge_dicts
from ray.rllib.utils.actors import create_colocated
from ray.rllib.utils.experimental_dsl import (
    ParallelRollouts, Concurrently, StoreToReplayBuffer, LocalReplay,
    ParallelReplay, TrainOneStep, StandardMetricsReporting,
    StoreToReplayActors,
    UpdateTargetNetwork)

# yapf: disable
# __sphinx_doc_begin__
APEX_DEFAULT_CONFIG = merge_dicts(
    DQN_CONFIG,  # see also the options in dqn.py, which are also supported
    {
        "optimizer": merge_dicts(
            DQN_CONFIG["optimizer"], {
                "max_weight_sync_delay": 400,
                "num_replay_buffer_shards": 4,
                "debug": False
            }),
        "n_step": 3,
        "num_gpus": 1,
        "num_workers": 32,
        "buffer_size": 2000000,
        "learning_starts": 50000,
        "train_batch_size": 512,
        "sample_batch_size": 50,
        "target_network_update_freq": 500000,
        "timesteps_per_iteration": 25000,
        "exploration_config": {"type": "PerWorkerEpsilonGreedy"},
        "worker_side_prioritization": True,
        "min_iter_time_s": 30,
    },
)
# __sphinx_doc_end__
# yapf: enable


def defer_make_workers(trainer, env_creator, policy, config):
    # Hack to workaround https://github.com/ray-project/ray/issues/2541
    # The workers will be created later, after the optimizer is created
    return trainer._make_workers(env_creator, policy, config, 0)


def make_async_optimizer(workers, config):
    assert len(workers.remote_workers()) == 0
    extra_config = config["optimizer"].copy()
    for key in [
            "prioritized_replay", "prioritized_replay_alpha",
            "prioritized_replay_beta", "prioritized_replay_eps"
    ]:
        if key in config:
            extra_config[key] = config[key]
    opt = AsyncReplayOptimizer(
        workers,
        learning_starts=config["learning_starts"],
        buffer_size=config["buffer_size"],
        train_batch_size=config["train_batch_size"],
        sample_batch_size=config["sample_batch_size"],
        **extra_config)
    workers.add_workers(config["num_workers"])
    opt._set_workers(workers.remote_workers())
    return opt


def update_target_based_on_num_steps_trained(trainer, fetches):
    # Ape-X updates based on num steps trained, not sampled
    if (trainer.optimizer.num_steps_trained -
            trainer.state["last_target_update_ts"] >
            trainer.config["target_network_update_freq"]):
        trainer.workers.local_worker().foreach_trainable_policy(
            lambda p, _: p.update_target())
        trainer.state["last_target_update_ts"] = (
            trainer.optimizer.num_steps_trained)
        trainer.state["num_target_updates"] += 1


# Experimental pipeline-based impl; enable with "use_pipeline_impl": True.
def training_pipeline(workers, config):
    num_replay_buffer_shards = config["optimizer"]["num_replay_buffer_shards"]
    replay_actors = create_colocated(ReplayActor, [
        num_replay_buffer_shards,
        config["learning_starts"],
        config["buffer_size"],
        config["train_batch_size"],
        config["prioritized_replay_alpha"],
        config["prioritized_replay_beta"],
        config["prioritized_replay_eps"],
    ], num_replay_buffer_shards)

    # (1) Store experiences into the local replay buffer.
    rollouts = ParallelRollouts(workers)
    store_op = rollouts.for_each(StoreToReplayActors(replay_actors))

    # (2) Replay and train on selected experiences.
    replay_op = ParallelReplay(replay_actors) \
        .for_each(Enqueue(in_queue)) \

    LearnerThread(in_queue, out_queue).start()
    
    stats_op = Dequeue(out_queue) \
        .for_each(UpdateTargetNetwork(
            workers, config["target_network_update_freq"],
            by_steps_trained=True))

    # Alternate deterministically between (1) and (2).
    train_op = Concurrently([store_op, replay_op], mode="async")

    return StandardMetricsReporting(train_op, workers, config)


APEX_TRAINER_PROPERTIES = {
    "make_workers": defer_make_workers,
    "make_policy_optimizer": make_async_optimizer,
    "after_optimizer_step": update_target_based_on_num_steps_trained,
}

ApexTrainer = DQNTrainer.with_updates(
    name="APEX",
    default_config=APEX_DEFAULT_CONFIG,
    training_pipeline=training_pipeline,
    **APEX_TRAINER_PROPERTIES)
