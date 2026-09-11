"""Example using a `SingleAgentObservationPreprocessor` to preprocess observations.

The custom preprocessor here is part of the env-to-module connector pipeline and
alters the CartPole-v1 environment observations from the Markovian 4-tuple (x-pos,
angular-pos, x-velocity, angular-velocity) to a non-Markovian, simpler 2-tuple (only
x-pos and angular-pos). The resulting problem can only be solved through a
memory/stateful model, for example an LSTM.

An RLlib Algorithm has 3 distinct connector pipelines:
- An env-to-module pipeline in an EnvRunner accepting a list of episodes and producing
a batch for an RLModule to compute actions (`forward_inference()` or
`forward_exploration()`).
- A module-to-env pipeline in an EnvRunner taking the RLModule's output and converting
it into an action readable by the environment.
- A learner connector pipeline on a Learner taking a list of episodes and producing
a batch for an RLModule to perform the training forward pass (`forward_train()`).

Each of these pipelines has a fixed set of default ConnectorV2 pieces that RLlib
adds/prepends to these pipelines in order to perform the most basic functionalities.
For example, RLlib adds the `AddObservationsFromEpisodesToBatch` ConnectorV2 into any
env-to-module pipeline to make sure the batch for computing actions contains - at the
minimum - the most recent observation.

On top of these default ConnectorV2 pieces, users can define their own ConnectorV2
pieces (or use the ones available already in RLlib) and add them to one of the 3
different pipelines described above, as required.

This example:
    - shows how to write a custom `SingleAgentObservationPreprocessor` ConnectorV2
    piece.
    - shows how to add this custom class to the env-to-module pipeline through the
    algorithm config.
    - demonstrates that by using this connector, the normal CartPole observation
    changes from a Markovian (fully observable) to a non-Markovian (partially
    observable) observation. Only stateful, memory enhanced models can solve the
    resulting RL problem.


How to run this script
----------------------
`python [script file name].py`

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
--wandb-run-name=[optional: WandB run name (within the defined project)]`


Results to expect
-----------------

You should see something like this at the end in your console output.
Note that your setup wouldn't be able to solve the environment, preprocessed through
your custom `SingleAgentObservationPreprocessor`, without the help of the configured
LSTM since you convert the env from a Markovian one to a partially observable,
non-Markovian one.
+-----------------------------+------------+-----------------+--------+
| Trial name                  | status     | loc             |   iter |
|                             |            |                 |        |
|-----------------------------+------------+-----------------+--------+
| PPO_CartPole-v1_0ecb5_00000 | TERMINATED | 127.0.0.1:57921 |      9 |
+-----------------------------+------------+-----------------+--------+
+------------------+------------------------+------------------------+
|   total time (s) |    episode_return_mean |   num_env_steps_sample |
|                  |                        |             d_lifetime |
|------------------+------------------------+------------------------|
|          26.2305 |                 224.38 |                  36000 |
+------------------+------------------------+------------------------+
"""
import gymnasium as gym
import numpy as np

from ray.rllib.connectors.env_to_module.observation_preprocessor import (
    SingleAgentObservationPreprocessor,
)
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.rllib.examples.utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.tune.registry import get_trainable_cls

# Read in common example script command line arguments.
parser = add_rllib_example_script_args(default_timesteps=200000, default_reward=200.0)


class ReduceCartPoleObservationsToNonMarkovian(SingleAgentObservationPreprocessor):
    def recompute_output_observation_space(
        self,
        input_observation_space: gym.Space,
        input_action_space: gym.Space,
    ) -> gym.Space:
        # The new observation space only has a shape of (2,), not (4,).
        return gym.spaces.Box(
            -5.0,
            5.0,
            (input_observation_space.shape[0] - 2,),
            np.float32,
        )

    def preprocess(self, observation, episode: SingleAgentEpisode):
        # Extract only the positions (x-position and angular-position).
        return np.array([observation[0], observation[2]], np.float32)


if __name__ == "__main__":
    args = parser.parse_args()

    # Define the AlgorithmConfig used.
    base_config = (
        get_trainable_cls(args.algo)
        .get_default_config()
        # You use the normal CartPole-v1 env here and your env-to-module preprocessor
        # converts this into a non-Markovian version of CartPole.
        .environment("CartPole-v1")
        .env_runners(
            env_to_module_connector=(
                lambda env, spaces, device: ReduceCartPoleObservationsToNonMarkovian()
            ),
        )
        .training(
            gamma=0.99,
            lr=0.0003,
        )
        .rl_module(
            model_config=DefaultModelConfig(
                # Solve the non-Markovian env through using an LSTM-enhanced model.
                use_lstm=True,
                vf_share_layers=True,
            ),
        )
    )

    # PPO-specific settings (for better learning behavior only).
    if args.algo == "PPO":
        base_config.training(
            num_epochs=6,
            vf_loss_coeff=0.01,
        )
    # IMPALA-specific settings (for better learning behavior only).
    elif args.algo == "IMPALA":
        base_config.training(
            lr=0.0005,
            vf_loss_coeff=0.05,
            entropy_coeff=0.0,
        )

    # Run everything as configured.
    run_rllib_example_script_experiment(base_config, args)
