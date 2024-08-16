# @OldAPIStack

"""Example on how to use CQL to learn from an offline JSON file.

Important node: Make sure that your offline data file contains only
a single timestep per line to mimic the way SAC pulls samples from
the buffer.

Generate the offline json file by running an SAC algo until it reaches expert
level on your command line. For example:
$ cd ray
$ rllib train -f rllib/tuned_examples/sac/pendulum-sac.yaml --no-ray-ui

Also make sure that in the above SAC yaml file (pendulum-sac.yaml),
you specify an additional "output" key with any path on your local
file system. In that path, the offline json files will be written to.

Use the generated file(s) as "input" in the CQL config below
(`config["input"] = [list of your json files]`), then run this script.
"""

import argparse
import numpy as np

from ray.rllib.policy.sample_batch import convert_ma_batch_to_sample_batch
from ray.rllib.algorithms import cql as cql
from ray.rllib.execution.rollout_ops import (
    synchronous_parallel_sample,
)
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    EVALUATION_RESULTS,
)

torch, _ = try_import_torch()

parser = argparse.ArgumentParser()
parser.add_argument(
    "--as-test",
    action="store_true",
    help="Whether this script should be run as a test: --stop-reward must "
    "be achieved within --stop-timesteps AND --stop-iters.",
)
parser.add_argument(
    "--stop-iters", type=int, default=5, help="Number of iterations to train."
)
parser.add_argument(
    "--stop-reward", type=float, default=50.0, help="Reward at which we stop training."
)


if __name__ == "__main__":
    args = parser.parse_args()

    # See rllib/tuned_examples/cql/pendulum-cql.yaml for comparison.
    config = (
        cql.CQLConfig()
        .framework(framework="torch")
        .env_runners(num_env_runners=0)
        .training(
            n_step=3,
            bc_iters=0,
            clip_actions=False,
            tau=0.005,
            target_entropy="auto",
            q_model_config={
                "fcnet_hiddens": [256, 256],
                "fcnet_activation": "relu",
            },
            policy_model_config={
                "fcnet_hiddens": [256, 256],
                "fcnet_activation": "relu",
            },
            optimization_config={
                "actor_learning_rate": 3e-4,
                "critic_learning_rate": 3e-4,
                "entropy_learning_rate": 3e-4,
            },
            train_batch_size=256,
            target_network_update_freq=1,
            num_steps_sampled_before_learning_starts=256,
        )
        .reporting(min_train_timesteps_per_iteration=1000)
        .debugging(log_level="INFO")
        .environment("Pendulum-v1", normalize_actions=True)
        .offline_data(
            input_config={
                "paths": ["tests/data/pendulum/enormous.zip"],
                "format": "json",
            }
        )
        .evaluation(
            evaluation_num_env_runners=1,
            evaluation_interval=1,
            evaluation_duration=10,
            evaluation_parallel_to_training=False,
            evaluation_config=cql.CQLConfig.overrides(input_="sampler"),
        )
    )
    # evaluation_parallel_to_training should be False b/c iterations are very long
    # and this would cause evaluation to lag one iter behind training.

    # Check, whether we can learn from the given file in `num_iterations`
    # iterations, up to a reward of `min_reward`.
    num_iterations = 5
    min_reward = -300

    # Test for torch framework (tf not implemented yet).
    cql_algorithm = cql.CQL(config=config)
    learnt = False
    for i in range(num_iterations):
        print(f"Iter {i}")
        eval_results = cql_algorithm.train().get(EVALUATION_RESULTS)
        if eval_results:
            print(
                "... R={}".format(eval_results[ENV_RUNNER_RESULTS][EPISODE_RETURN_MEAN])
            )
            # Learn until some reward is reached on an actual live env.
            if eval_results[ENV_RUNNER_RESULTS][EPISODE_RETURN_MEAN] >= min_reward:
                # Test passed gracefully.
                if args.as_test:
                    print("Test passed after {} iterations.".format(i))
                    quit(0)
                learnt = True
                break

    # Get policy and model.
    cql_policy = cql_algorithm.get_policy()
    cql_model = cql_policy.model

    # If you would like to query CQL's learnt Q-function for arbitrary
    # (cont.) actions, do the following:
    obs_batch = torch.from_numpy(np.random.random(size=(5, 3)))
    action_batch = torch.from_numpy(np.random.random(size=(5, 1)))
    q_values = cql_model.get_q_values(obs_batch, action_batch)[0]
    # If you are using the "twin_q", there'll be 2 Q-networks and
    # we usually consider the min of the 2 outputs, like so:
    twin_q_values = cql_model.get_twin_q_values(obs_batch, action_batch)[0]
    final_q_values = torch.min(q_values, twin_q_values)[0]
    print(f"final_q_values={final_q_values.detach().numpy()}")

    # Example on how to do evaluation on the trained Algorithm.
    # using the data from our buffer.
    # Get a sample (MultiAgentBatch).

    batch = synchronous_parallel_sample(worker_set=cql_algorithm.env_runner_group)
    batch = convert_ma_batch_to_sample_batch(batch)
    obs = torch.from_numpy(batch["obs"])
    # Pass the observations through our model to get the
    # features, which then to pass through the Q-head.
    model_out, _ = cql_model({"obs": obs})
    # The estimated Q-values from the (historic) actions in the batch.
    q_values_old = cql_model.get_q_values(
        model_out, torch.from_numpy(batch["actions"])
    )[0]
    # The estimated Q-values for the new actions computed by our policy.
    actions_new = cql_policy.compute_actions_from_input_dict({"obs": obs})[0]
    q_values_new = cql_model.get_q_values(model_out, torch.from_numpy(actions_new))[0]
    print(f"Q-val batch={q_values_old.detach().numpy()}")
    print(f"Q-val policy={q_values_new.detach().numpy()}")

    cql_algorithm.stop()
