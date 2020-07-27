"""Example of using a custom training workflow.

Here we create a number of CartPole agents, some of which are trained with
DQN, and some of which are trained with PPO. Both are executed concurrently
via a custom training workflow.
"""

import argparse
import gym

import ray
from ray import tune
from ray.rllib.agents.trainer_template import build_trainer
from ray.rllib.agents.dqn.dqn import DEFAULT_CONFIG as DQN_CONFIG
from ray.rllib.agents.dqn.dqn_tf_policy import DQNTFPolicy
from ray.rllib.agents.dqn.dqn_torch_policy import DQNTorchPolicy
from ray.rllib.agents.ppo.ppo import DEFAULT_CONFIG as PPO_CONFIG
from ray.rllib.agents.ppo.ppo_tf_policy import PPOTFPolicy
from ray.rllib.agents.ppo.ppo_torch_policy import PPOTorchPolicy
from ray.rllib.evaluation.worker_set import WorkerSet
from ray.rllib.execution.common import _get_shared_metrics
from ray.rllib.execution.concurrency_ops import Concurrently
from ray.rllib.execution.metric_ops import StandardMetricsReporting
from ray.rllib.execution.rollout_ops import ParallelRollouts, ConcatBatches, \
    StandardizeFields, SelectExperiences
from ray.rllib.execution.replay_ops import StoreToReplayBuffer, Replay
from ray.rllib.execution.train_ops import TrainOneStep, UpdateTargetNetwork
from ray.rllib.execution.replay_buffer import LocalReplayBuffer
from ray.rllib.examples.env.multi_agent import MultiAgentCartPole
from ray.rllib.utils.test_utils import check_learning_achieved
from ray.tune.registry import register_env

parser = argparse.ArgumentParser()
parser.add_argument("--as-test", action="store_true")
parser.add_argument("--torch", action="store_true")
parser.add_argument("--mixed-torch-tf", action="store_true")
parser.add_argument("--stop-iters", type=int, default=20)
parser.add_argument("--stop-reward", type=float, default=150.0)
parser.add_argument("--stop-timesteps", type=int, default=100000)


def custom_training_workflow(workers: WorkerSet, config: dict):
    local_replay_buffer = LocalReplayBuffer(
        num_shards=1,
        learning_starts=1000,
        buffer_size=50000,
        replay_batch_size=64)

    def add_ppo_metrics(batch):
        print("PPO policy learning on samples from",
              batch.policy_batches.keys(), "env steps", batch.count,
              "agent steps", batch.total())
        metrics = _get_shared_metrics()
        metrics.counters["agent_steps_trained_PPO"] += batch.total()
        return batch

    def add_dqn_metrics(batch):
        print("DQN policy learning on samples from",
              batch.policy_batches.keys(), "env steps", batch.count,
              "agent steps", batch.total())
        metrics = _get_shared_metrics()
        metrics.counters["agent_steps_trained_DQN"] += batch.total()
        return batch

    # Generate common experiences.
    rollouts = ParallelRollouts(workers, mode="bulk_sync")
    r1, r2 = rollouts.duplicate(n=2)

    # DQN sub-flow.
    dqn_store_op = r1.for_each(SelectExperiences(["dqn_policy"])) \
        .for_each(
            StoreToReplayBuffer(local_buffer=local_replay_buffer))
    dqn_replay_op = Replay(local_buffer=local_replay_buffer) \
        .for_each(add_dqn_metrics) \
        .for_each(TrainOneStep(workers, policies=["dqn_policy"])) \
        .for_each(UpdateTargetNetwork(
            workers, target_update_freq=500, policies=["dqn_policy"]))
    dqn_train_op = Concurrently(
        [dqn_store_op, dqn_replay_op], mode="round_robin", output_indexes=[1])

    # PPO sub-flow.
    ppo_train_op = r2.for_each(SelectExperiences(["ppo_policy"])) \
        .combine(ConcatBatches(min_batch_size=200)) \
        .for_each(add_ppo_metrics) \
        .for_each(StandardizeFields(["advantages"])) \
        .for_each(TrainOneStep(
            workers,
            policies=["ppo_policy"],
            num_sgd_iter=10,
            sgd_minibatch_size=128))

    # Combined training flow
    train_op = Concurrently(
        [ppo_train_op, dqn_train_op], mode="async", output_indexes=[1])

    return StandardMetricsReporting(train_op, workers, config)


if __name__ == "__main__":
    args = parser.parse_args()
    assert not (args.torch and args.mixed_torch_tf),\
        "Use either --torch or --mixed-torch-tf, not both!"

    ray.init()

    # Simple environment with 4 independent cartpole entities
    register_env("multi_agent_cartpole",
                 lambda _: MultiAgentCartPole({"num_agents": 4}))
    single_env = gym.make("CartPole-v0")
    obs_space = single_env.observation_space
    act_space = single_env.action_space

    # Note that since the trainer below does not include a default policy or
    # policy configs, we have to explicitly set it in the multiagent config:
    policies = {
        "ppo_policy": (PPOTorchPolicy if args.torch or args.mixed_torch_tf else
                       PPOTFPolicy, obs_space, act_space, PPO_CONFIG),
        "dqn_policy": (DQNTorchPolicy if args.torch else DQNTFPolicy,
                       obs_space, act_space, DQN_CONFIG),
    }

    def policy_mapping_fn(agent_id):
        if agent_id % 2 == 0:
            return "ppo_policy"
        else:
            return "dqn_policy"

    MyTrainer = build_trainer(
        name="PPO_DQN_MultiAgent",
        default_policy=None,
        execution_plan=custom_training_workflow)

    config = {
        "rollout_fragment_length": 50,
        "num_workers": 0,
        "env": "multi_agent_cartpole",
        "multiagent": {
            "policies": policies,
            "policy_mapping_fn": policy_mapping_fn,
            "policies_to_train": ["dqn_policy", "ppo_policy"],
        },
        "framework": "torch" if args.torch else "tf",
    }

    stop = {
        "training_iteration": args.stop_iters,
        "timesteps_total": args.stop_timesteps,
        "episode_reward_mean": args.stop_reward,
    }

    results = tune.run(MyTrainer, config=config, stop=stop)

    if args.as_test:
        check_learning_achieved(results, args.stop_reward)

    ray.shutdown()
