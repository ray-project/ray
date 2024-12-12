"""Example of implementing and training with an intrinsic curiosity model (ICM).

This type of curiosity-based learning trains a simplified model of the environment
dynamics based on three networks:
1) Embedding observations into latent space ("feature" network).
2) Predicting the action, given two consecutive embedded observations
("inverse" network).
3) Predicting the next embedded obs, given an obs and action
("forward" network).

The less the ICM is able to predict the actually observed next feature vector,
given obs and action (through the forwards network), the larger the
"intrinsic reward", which will be added to the extrinsic reward of the agent.

Therefore, if a state transition was unexpected, the agent becomes
"curious" and will further explore this transition leading to better
exploration in sparse rewards environments.

For more details, see here:
[1] Curiosity-driven Exploration by Self-supervised Prediction
Pathak, Agrawal, Efros, and Darrell - UC Berkeley - ICML 2017.
https://arxiv.org/pdf/1705.05363.pdf

This example:
    - demonstrates how to write a custom RLModule, representing the ICM from the paper
    above. Note that this custom RLModule does not belong to any individual agent.
    - demonstrates how to write a custom (PPO) TorchLearner that a) adds the ICM to its
    MultiRLModule, b) trains the regular PPO Policy plus the ICM module, using the
    PPO parent loss and the ICM's RLModule's own loss function.

We use a FrozenLake (sparse reward) environment with a custom map size of 12x12 and a
hard time step limit of 22 to make it almost impossible for a non-curiosity based
learners to learn a good policy.


How to run this script
----------------------
`python [script file name].py --enable-new-api-stack`

Use the `--no-curiosity` flag to disable curiosity learning and force your policy
to be trained on the task w/o the use of intrinsic rewards. With this option, the
algorithm should NOT succeed.

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
--wandb-run-name=[optional: WandB run name (within the defined project)]`


Results to expect
-----------------
In the console output, you can see that only a PPO policy that uses curiosity can
actually learn.

Policy using ICM-based curiosity:
+-------------------------------+------------+-----------------+--------+
| Trial name                    | status     | loc             |   iter |
|-------------------------------+------------+-----------------+--------+
| PPO_FrozenLake-v1_52ab2_00000 | TERMINATED | 127.0.0.1:73318 |    392 |
+-------------------------------+------------+-----------------+--------+
+------------------+--------+----------+--------------------+
|   total time (s) |     ts |   reward |   episode_len_mean |
|------------------+--------+----------+--------------------|
|          236.652 | 786000 |      1.0 |               22.0 |
+------------------+--------+----------+--------------------+

Policy NOT using curiosity:
[DOES NOT LEARN AT ALL]
"""
from collections import defaultdict

import numpy as np

from ray import tune
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.algorithms.callbacks import DefaultCallbacks
from ray.rllib.connectors.env_to_module import FlattenObservations
from ray.rllib.examples.learners.classes.intrinsic_curiosity_learners import (
    DQNTorchLearnerWithCuriosity,
    PPOTorchLearnerWithCuriosity,
)
from ray.rllib.core import DEFAULT_MODULE_ID
from ray.rllib.core.rl_module.multi_rl_module import MultiRLModuleSpec
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.examples.learners.classes.intrinsic_curiosity_learners import (
    ICM_MODULE_ID,
)
from ray.rllib.examples.rl_modules.classes.intrinsic_curiosity_model_rlm import (
    IntrinsicCuriosityModel,
)
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
)
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)

parser = add_rllib_example_script_args(
    default_iters=2000,
    default_timesteps=10000000,
    default_reward=0.9,
)
parser.set_defaults(enable_new_api_stack=True)


class MeasureMaxDistanceToStart(DefaultCallbacks):
    """Callback measuring the dist of the agent to its start position in FrozenLake-v1.

    Makes the naive assumption that the start position ("S") is in the upper left
    corner of the used map.
    Uses the MetricsLogger to record the (euclidian) distance value.
    """

    def __init__(self):
        super().__init__()
        self.max_dists = defaultdict(float)
        self.max_dists_lifetime = 0.0

    def on_episode_step(
        self,
        *,
        episode,
        env_runner,
        metrics_logger,
        env,
        env_index,
        rl_module,
        **kwargs,
    ):
        num_rows = env.envs[0].unwrapped.nrow
        num_cols = env.envs[0].unwrapped.ncol
        obs = np.argmax(episode.get_observations(-1))
        row = obs // num_cols
        col = obs % num_rows
        curr_dist = (row**2 + col**2) ** 0.5
        if curr_dist > self.max_dists[episode.id_]:
            self.max_dists[episode.id_] = curr_dist

    def on_episode_end(
        self,
        *,
        episode,
        env_runner,
        metrics_logger,
        env,
        env_index,
        rl_module,
        **kwargs,
    ):
        # Compute current maximum distance across all running episodes
        # (including the just ended one).
        max_dist = max(self.max_dists.values())
        metrics_logger.log_value(
            key="max_dist_travelled_across_running_episodes",
            value=max_dist,
            window=10,
        )
        if max_dist > self.max_dists_lifetime:
            self.max_dists_lifetime = max_dist
        del self.max_dists[episode.id_]

    def on_sample_end(
        self,
        *,
        env_runner,
        metrics_logger,
        samples,
        **kwargs,
    ):
        metrics_logger.log_value(
            key="max_dist_travelled_lifetime",
            value=self.max_dists_lifetime,
            window=1,
        )


if __name__ == "__main__":
    args = parser.parse_args()

    if args.algo not in ["DQN", "PPO"]:
        raise ValueError(
            "Curiosity example only implemented for either DQN or PPO! See the "
        )

    base_config = (
        tune.registry.get_trainable_cls(args.algo)
        .get_default_config()
        .environment(
            "FrozenLake-v1",
            env_config={
                # Use a 12x12 map.
                "desc": [
                    "SFFFFFFFFFFF",
                    "FFFFFFFFFFFF",
                    "FFFFFFFFFFFF",
                    "FFFFFFFFFFFF",
                    "FFFFFFFFFFFF",
                    "FFFFFFFFFFFF",
                    "FFFFFFFFFFFF",
                    "FFFFFFFFFFFF",
                    "FFFFFFFFFFFF",
                    "FFFFFFFFFFFF",
                    "FFFFFFFFFFFF",
                    "FFFFFFFFFFFG",
                ],
                "is_slippery": False,
                # Limit the number of steps the agent is allowed to make in the env to
                # make it almost impossible to learn without the curriculum.
                "max_episode_steps": 22,
            },
        )
        .callbacks(MeasureMaxDistanceToStart)
        .env_runners(
            num_envs_per_env_runner=5 if args.algo == "PPO" else 1,
            env_to_module_connector=lambda env: FlattenObservations(),
        )
        .training(
            learner_config_dict={
                # Intrinsic reward coefficient.
                "intrinsic_reward_coeff": 0.05,
                # Forward loss weight (vs inverse dynamics loss). Total ICM loss is:
                # L(total ICM) = (
                #     `forward_loss_weight` * L(forward)
                #     + (1.0 - `forward_loss_weight`) * L(inverse_dyn)
                # )
                "forward_loss_weight": 0.2,
            }
        )
        .rl_module(
            rl_module_spec=MultiRLModuleSpec(
                rl_module_specs={
                    # The "main" RLModule (policy) to be trained by our algo.
                    DEFAULT_MODULE_ID: RLModuleSpec(
                        **(
                            {"model_config": {"vf_share_layers": True}}
                            if args.algo == "PPO"
                            else {}
                        ),
                    ),
                    # The intrinsic curiosity model.
                    ICM_MODULE_ID: RLModuleSpec(
                        module_class=IntrinsicCuriosityModel,
                        # Only create the ICM on the Learner workers, NOT on the
                        # EnvRunners.
                        learner_only=True,
                        # Configure the architecture of the ICM here.
                        model_config={
                            "feature_dim": 288,
                            "feature_net_hiddens": (256, 256),
                            "feature_net_activation": "relu",
                            "inverse_net_hiddens": (256, 256),
                            "inverse_net_activation": "relu",
                            "forward_net_hiddens": (256, 256),
                            "forward_net_activation": "relu",
                        },
                    ),
                }
            ),
            # Use a different learning rate for training the ICM.
            algorithm_config_overrides_per_module={
                ICM_MODULE_ID: AlgorithmConfig.overrides(lr=0.0005)
            },
        )
    )

    # Set PPO-specific hyper-parameters.
    if args.algo == "PPO":
        base_config.training(
            num_epochs=6,
            # Plug in the correct Learner class.
            learner_class=PPOTorchLearnerWithCuriosity,
            train_batch_size_per_learner=2000,
            lr=0.0003,
        )
    elif args.algo == "DQN":
        base_config.training(
            # Plug in the correct Learner class.
            learner_class=DQNTorchLearnerWithCuriosity,
            train_batch_size_per_learner=128,
            lr=0.00075,
            replay_buffer_config={
                "type": "PrioritizedEpisodeReplayBuffer",
                "capacity": 500000,
                "alpha": 0.6,
                "beta": 0.4,
            },
            # Epsilon exploration schedule for DQN.
            epsilon=[[0, 1.0], [500000, 0.05]],
            n_step=(3, 5),
            double_q=True,
            dueling=True,
        )

    success_key = f"{ENV_RUNNER_RESULTS}/max_dist_travelled_across_running_episodes"
    stop = {
        success_key: 12.0,
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": args.stop_reward,
        NUM_ENV_STEPS_SAMPLED_LIFETIME: args.stop_timesteps,
    }

    run_rllib_example_script_experiment(
        base_config,
        args,
        stop=stop,
        success_metric={success_key: stop[success_key]},
    )
