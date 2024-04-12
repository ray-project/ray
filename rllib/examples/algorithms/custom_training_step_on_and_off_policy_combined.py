"""Example of using a custom training workflow via overriding `Algorithm.training_step`.

This example:
- defines a new Algorithm subclass and implements its `setup` (to add a DQN replay
buffer) and `training_step` (to define a custom workflow using both on-policy and off-
policy learning).
- uses a multi-agent CartPole environment, in which N agents map to N RLModules,
some of which are trained using PPO (on-policy) and some are trained by DQN
(off-policy).
- training all RLModules (policies) is performed concurrently using the above mentioned
custom training workflow (defined in `training_step()`).


How to run this script
----------------------
`python [script file name].py --enable-new-api-stack --num-agents=2`

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
--wandb-run-name=[optional: WandB run name (within the defined project)]`


Results to expect
-----------------
In the console output, you can see the PPO policy ("learnable_policy") does much
better than "random":

+-------------------+------------+----------+------+----------------+
| Trial name        | status     | loc      | iter | total time (s) |
|                   |            |          |      |                |
|-------------------+------------+----------+------+----------------+
| PPO_multi_agen... | TERMINATED | 127. ... |   20 |         58.646 |
+-------------------+------------+----------+------+----------------+

+--------+-------------------+-----------------+--------------------+
|     ts |   combined reward |   reward random |             reward |
|        |                   |                 |   learnable_policy |
+--------+-------------------+-----------------+--------------------|
|  80000 |            481.26 |           78.41 |             464.41 |
+--------+-------------------+-----------------+--------------------+
"""

from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
from ray.rllib.core.rl_module.marl_module import MultiAgentRLModuleSpec
from ray.rllib.examples.envs.classes.multi_agent import MultiAgentCartPole
from ray.rllib.examples.rl_modules.classes.random_rlm import RandomRLModule
from ray.rllib.utils.annotations import override
from ray.rllib.utils.metrics import (
    NUM_AGENT_STEPS_SAMPLED,
    NUM_ENV_STEPS_SAMPLED,
    NUM_TARGET_UPDATES,
    LAST_TARGET_UPDATE_TS,
)
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.rllib.utils.typing import ResultDict
from ray.tune.registry import register_env


parser = add_rllib_example_script_args(
    default_iters=600, default_reward=600.0, default_timesteps=200000
)


# Define new Algorithm with custom `training_step()` method (training workflow).
class MyOnPolicyOffPolicyAlgo(Algorithm):
    @override(Algorithm)
    def setup(self, config):
        # Call super's `setup` to create rollout workers.
        super().setup(config)
        # TODO (sven): Once we have a multi-agent capable replay buffer, use it here.
        #  For now, we are ok with a separate buffer per DQN agent (as we are doing
        #  independent learning anyways).
        # Create N local replay buffers (one for each DQN agent).
        self.local_replay_buffers = [
            EpisodeMultiAgentReplayBuffer(num_shards=1, capacity=50000)
            for n in range(0, args.num_agents, 2)
        ]

    @override(Algorithm)
    def training_step(self) -> ResultDict:
        # Generate common experiences, collect batch for PPO, store every (DQN) batch
        # into replay buffer.
        ppo_batches = []
        num_env_steps = 0

        # PPO batch size fixed at 200.
        # TODO: Use `max_env_steps=200` option of synchronous_parallel_sample instead.
        while num_env_steps < 200:
            ma_batches = synchronous_parallel_sample(
                worker_set=self.workers, concat=False
            )
            # Loop through ma-batches (which were collected in parallel).
            for ma_batch in ma_batches:
                # Update sampled counters.
                self._counters[NUM_ENV_STEPS_SAMPLED] += ma_batch.count
                self._counters[NUM_AGENT_STEPS_SAMPLED] += ma_batch.agent_steps()
                # Add collected batches (only for DQN policy) to replay buffer.
                ppo_batch = ma_batch.policy_batches.pop("ppo_policy")
                self.local_replay_buffer.add(ma_batch)

                ppo_batches.append(ppo_batch)
                num_env_steps += ppo_batch.count

        # DQN sub-flow.
        dqn_train_results = {}
        # Start updating DQN policy once we have some samples in the buffer.
        if self._counters[NUM_ENV_STEPS_SAMPLED] > 1000:
            # Update DQN policy n times while updating PPO policy once.
            for _ in range(10):
                dqn_train_batch = self.local_replay_buffer.sample(num_items=64)
                dqn_train_results = train_one_step(
                    self, dqn_train_batch, ["dqn_policy"]
                )
                self._counters[
                    "agent_steps_trained_DQN"
                ] += dqn_train_batch.agent_steps()
                print(
                    "DQN policy learning on samples from",
                    "agent steps trained",
                    dqn_train_batch.agent_steps(),
                )
        # Update DQN's target net every n train steps (determined by the DQN config).
        if (
            self._counters["agent_steps_trained_DQN"]
            - self._counters[LAST_TARGET_UPDATE_TS]
            >= self.get_policy("dqn_policy").config["target_network_update_freq"]
        ):
            self.workers.local_worker().get_policy("dqn_policy").update_target()
            self._counters[NUM_TARGET_UPDATES] += 1
            self._counters[LAST_TARGET_UPDATE_TS] = self._counters[
                "agent_steps_trained_DQN"
            ]

        # PPO sub-flow.
        ppo_train_batch = concat_samples(ppo_batches)
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

    # Simple environment with 4 independent cartpole entities
    register_env("env", lambda cfg: MultiAgentCartPole({"num_agents": 4, **cfg}))

    # Note that since the algorithm below does not include a default policy or
    # policy configs, we have to explicitly set it in the multi_agent config:
    policies = {
        "ppo_policy": (
            PPOTorchPolicy if args.torch or args.mixed_torch_tf else PPOTF1Policy,
            None,
            None,
            # Provide entire AlgorithmConfig object, not just an override.
            PPOConfig()
            .training(num_sgd_iter=10, sgd_minibatch_size=128)
            .framework("torch" if args.torch or args.mixed_torch_tf else "tf"),
        ),
        "dqn_policy": (
            DQNTorchPolicy if args.torch else DQNTFPolicy,
            None,
            None,
            # Provide entire AlgorithmConfig object, not just an override.
            DQNConfig().training(target_network_update_freq=500).framework("tf"),
        ),
    }

    def agent_to_module_mapping_fn(agent_id, episode, **kwargs):
        if agent_id % 2 == 0:
            return "ppo_module"
        else:
            return "dqn_module"

    base_config = (
        AlgorithmConfig()
        .environment("multi_agent_cartpole")
        .multi_agent(policies=policies, policy_mapping_fn=policy_mapping_fn)
        .rollouts(rollout_fragment_length=50)
        .reporting(metrics_num_episodes_for_smoothing=30)
    )

    run_rllib_example_script_experiment(base_config, args)

    base_config = (
        PPOConfig()
        .environment("multi_agent_cartpole")
        .multi_agent(
            policies={"learnable_policy", "random"},
            # Map to either random behavior or PPO learning behavior based on
            # the agent's ID.
            policy_mapping_fn=lambda agent_id, *args, **kwargs: [
                "learnable_policy",
                "random",
            ][agent_id % 2],
            # We need to specify this here, b/c the `forward_train` method of
            # `RandomRLModule` (ModuleID="random") throws a not-implemented error.
            policies_to_train=["learnable_policy"],
        )
        .rl_module(
            rl_module_spec=MultiAgentRLModuleSpec(
                module_specs={
                    "learnable_policy": SingleAgentRLModuleSpec(),
                    "random": SingleAgentRLModuleSpec(module_class=RandomRLModule),
                }
            ),
        )
    )

    run_rllib_example_script_experiment(base_config, args)
