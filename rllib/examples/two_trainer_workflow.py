"""Example of using a custom training workflow.

Here we create a number of CartPole agents, some of which are trained with
DQN, and some of which are trained with PPO. Both are executed concurrently
via a custom training workflow.
"""

import argparse
import os

import ray
from ray import tune
from ray.rllib.agents import with_common_config
from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.algorithms.dqn.dqn import DEFAULT_CONFIG as DQN_CONFIG
from ray.rllib.algorithms.dqn.dqn_tf_policy import DQNTFPolicy
from ray.rllib.algorithms.dqn.dqn_torch_policy import DQNTorchPolicy
from ray.rllib.algorithms.ppo.ppo import DEFAULT_CONFIG as PPO_CONFIG
from ray.rllib.algorithms.ppo.ppo_tf_policy import PPOTF1Policy
from ray.rllib.algorithms.ppo.ppo_torch_policy import PPOTorchPolicy
from ray.rllib.evaluation.postprocessing import Postprocessing
from ray.rllib.execution.rollout_ops import synchronous_parallel_sample
from ray.rllib.execution.train_ops import train_one_step
from ray.rllib.utils.replay_buffers.multi_agent_replay_buffer import (
    MultiAgentReplayBuffer,
)
from ray.rllib.examples.env.multi_agent import MultiAgentCartPole
from ray.rllib.policy.sample_batch import MultiAgentBatch, SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.metrics import (
    NUM_AGENT_STEPS_SAMPLED,
    NUM_ENV_STEPS_SAMPLED,
    NUM_TARGET_UPDATES,
    LAST_TARGET_UPDATE_TS,
)
from ray.rllib.utils.sgd import standardized
from ray.rllib.utils.test_utils import check_learning_achieved
from ray.rllib.utils.typing import ResultDict, AlgorithmConfigDict
from ray.tune.registry import register_env

parser = argparse.ArgumentParser()
parser.add_argument("--torch", action="store_true")
parser.add_argument("--mixed-torch-tf", action="store_true")
parser.add_argument(
    "--local-mode",
    action="store_true",
    help="Init Ray in local mode for easier debugging.",
)
parser.add_argument(
    "--as-test",
    action="store_true",
    help="Whether this script should be run as a test: --stop-reward must "
    "be achieved within --stop-timesteps AND --stop-iters.",
)
parser.add_argument(
    "--stop-iters", type=int, default=400, help="Number of iterations to train."
)
parser.add_argument(
    "--stop-timesteps", type=int, default=100000, help="Number of timesteps to train."
)
# 600.0 = 4 (num_agents) x 150.0
parser.add_argument(
    "--stop-reward", type=float, default=600.0, help="Reward at which we stop training."
)


# Define new Algorithm with custom execution_plan/workflow.
class MyAlgo(Algorithm):
    @classmethod
    @override(Algorithm)
    def get_default_config(cls) -> AlgorithmConfigDict:
        # Run this Algorithm with new `training_step` API and set some PPO-specific
        # parameters.
        return with_common_config(
            {
                "num_sgd_iter": 10,
                "sgd_minibatch_size": 128,
            }
        )

    @override(Algorithm)
    def setup(self, config):
        # Call super's `setup` to create rollout workers.
        super().setup(config)
        # Create local replay buffer.
        self.local_replay_buffer = MultiAgentReplayBuffer(
            num_shards=1, learning_starts=1000, capacity=50000
        )

    @override(Algorithm)
    def training_step(self) -> ResultDict:
        # Generate common experiences, collect batch for PPO, store every (DQN) batch
        # into replay buffer.
        ppo_batches = []
        num_env_steps = 0
        # PPO batch size fixed at 200.
        while num_env_steps < 200:
            ma_batches = synchronous_parallel_sample(
                worker_set=self.workers, concat=False
            )
            # Loop through (parallely collected) ma-batches.
            for ma_batch in ma_batches:
                # Update sampled counters.
                self._counters[NUM_ENV_STEPS_SAMPLED] += ma_batch.count
                self._counters[NUM_AGENT_STEPS_SAMPLED] += ma_batch.agent_steps()
                ppo_batch = ma_batch.policy_batches.pop("ppo_policy")
                # Add collected batches (only for DQN policy) to replay buffer.
                self.local_replay_buffer.add(ma_batch)

                ppo_batches.append(ppo_batch)
                num_env_steps += ppo_batch.count

        # DQN sub-flow.
        dqn_train_results = {}
        dqn_train_batch = self.local_replay_buffer.sample(num_items=64)
        if dqn_train_batch is not None:
            dqn_train_results = train_one_step(self, dqn_train_batch, ["dqn_policy"])
            self._counters["agent_steps_trained_DQN"] += dqn_train_batch.agent_steps()
            print(
                "DQN policy learning on samples from",
                "agent steps trained",
                dqn_train_batch.agent_steps(),
            )
        # Update DQN's target net every 500 train steps.
        if (
            self._counters["agent_steps_trained_DQN"]
            - self._counters[LAST_TARGET_UPDATE_TS]
            >= 500
        ):
            self.workers.local_worker().get_policy("dqn_policy").update_target()
            self._counters[NUM_TARGET_UPDATES] += 1
            self._counters[LAST_TARGET_UPDATE_TS] = self._counters[
                "agent_steps_trained_DQN"
            ]

        # PPO sub-flow.
        ppo_train_batch = SampleBatch.concat_samples(ppo_batches)
        self._counters["agent_steps_trained_PPO"] += ppo_train_batch.agent_steps()
        # Standardize advantages.
        ppo_train_batch[Postprocessing.ADVANTAGES] = standardized(
            ppo_train_batch[Postprocessing.ADVANTAGES]
        )
        print(
            "PPO policy learning on samples from",
            "agent steps trained",
            ppo_train_batch.agent_steps(),
        )
        ppo_train_batch = MultiAgentBatch(
            {"ppo_policy": ppo_train_batch}, ppo_train_batch.count
        )
        ppo_train_results = train_one_step(self, ppo_train_batch, ["ppo_policy"])

        # Combine results for PPO and DQN into one results dict.
        results = dict(ppo_train_results, **dqn_train_results)
        return results


if __name__ == "__main__":
    args = parser.parse_args()
    assert not (
        args.torch and args.mixed_torch_tf
    ), "Use either --torch or --mixed-torch-tf, not both!"

    ray.init(local_mode=args.local_mode)

    # Simple environment with 4 independent cartpole entities
    register_env(
        "multi_agent_cartpole", lambda _: MultiAgentCartPole({"num_agents": 4})
    )

    # framework can be changed, so removed the hardcoded framework key
    # from policy configs.
    ppo_config = PPO_CONFIG
    del ppo_config["framework"]
    dqn_config = DQN_CONFIG
    del dqn_config["framework"]

    # Note that since the algorithm below does not include a default policy or
    # policy configs, we have to explicitly set it in the multiagent config:
    policies = {
        "ppo_policy": (
            PPOTorchPolicy if args.torch or args.mixed_torch_tf else PPOTF1Policy,
            None,
            None,
            ppo_config,
        ),
        "dqn_policy": (
            DQNTorchPolicy if args.torch else DQNTFPolicy,
            None,
            None,
            dqn_config,
        ),
    }

    def policy_mapping_fn(agent_id, episode, worker, **kwargs):
        if agent_id % 2 == 0:
            return "ppo_policy"
        else:
            return "dqn_policy"

    config = {
        "rollout_fragment_length": 50,
        "num_workers": 0,
        "env": "multi_agent_cartpole",
        "multiagent": {
            "policies": policies,
            "policy_mapping_fn": policy_mapping_fn,
            "policies_to_train": ["dqn_policy", "ppo_policy"],
        },
        # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
        "num_gpus": int(os.environ.get("RLLIB_NUM_GPUS", "0")),
        "framework": "torch" if args.torch else "tf",
    }

    stop = {
        "training_iteration": args.stop_iters,
        "timesteps_total": args.stop_timesteps,
        "episode_reward_mean": args.stop_reward,
    }

    results = tune.run(MyAlgo, config=config, stop=stop)

    if args.as_test:
        check_learning_achieved(results, args.stop_reward)

    ray.shutdown()
