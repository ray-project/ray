"""Test whether a CQLTrainer can learn from an offline Pendulum-v0 file.

It does demonstrate, how to use CQL with a simple json offline file.

Important node: Make sure that your offline data file contains only
a single timestep per line to mimic the way SAC pulls samples from
the buffer.

Generate the offline json file by running an SAC algo until it reaches expert
level on your command line:
$ cd ray
$ rllib train -f rllib/tuned_examples/sac/pendulum-sac.yaml

Also make sure that in the above SAC yaml file, you specify an
additional "output" key with any path on your local file system.
In that path, the offline json file will be written to.

Use the generated file(s) as "input" in the CQL config below, then run
this script.
"""

import numpy as np
import os

from ray.rllib.agents import cql as cql
from ray.rllib.utils.framework import try_import_torch

torch, _ = try_import_torch()

if __name__ == "__main__":

    # See rllib/tuned_examples/cql/pendulum-cql.yaml for comparison.

    config = cql.CQL_DEFAULT_CONFIG.copy()
    config["num_workers"] = 0  # Run locally.
    config["horizon"] = 200
    config["soft_horizon"] = True
    config["no_done_at_end"] = True
    config["n_step"] = 3
    config["bc_iters"] = 0
    config["clip_actions"] = False
    config["normalize_actions"] = True
    config["learning_starts"] = 256
    config["rollout_fragment_length"] = 1
    config["prioritized_replay"] = False
    config["tau"] = 0.005
    config["target_entropy"] = "auto"
    config["Q_model"] = {
        "fcnet_hiddens": [256, 256],
        "fcnet_activation": "relu",
    }
    config["policy_model"] = {
        "fcnet_hiddens": [256, 256],
        "fcnet_activation": "relu",
    }
    config["optimization"] = {
        "actor_learning_rate": 3e-4,
        "critic_learning_rate": 3e-4,
        "entropy_learning_rate": 3e-4,
    }
    config["train_batch_size"] = 256
    config["target_network_update_freq"] = 1
    config["timesteps_per_iteration"] = 1000
    data_file = "/path/to/my/json_file.json"
    print("data_file={} exists={}".format(data_file,
                                          os.path.isfile(data_file)))
    config["input"] = [data_file]
    config["log_level"] = "INFO"
    config["env"] = "Pendulum-v0"

    # Set up evaluation.
    config["evaluation_num_workers"] = 1
    config["evaluation_interval"] = 1
    config["evaluation_num_episodes"] = 10
    # This should be False b/c iterations are very long and this would
    # cause evaluation to lag one iter behind training.
    config["evaluation_parallel_to_training"] = False
    # Evaluate on actual environment.
    config["evaluation_config"] = {"input": "sampler"}

    # Check, whether we can learn from the given file in `num_iterations`
    # iterations, up to a reward of `min_reward`.
    num_iterations = 50
    min_reward = -300

    # Test for torch framework (tf not implemented yet).
    trainer = cql.CQLTrainer(config=config)
    learnt = False
    for i in range(num_iterations):
        print(f"Iter {i}")
        eval_results = trainer.train().get("evaluation")
        if eval_results:
            print("... R={}".format(eval_results["episode_reward_mean"]))
            # Learn until some reward is reached on an actual live env.
            if eval_results["episode_reward_mean"] >= min_reward:
                learnt = True
                break
    if not learnt:
        raise ValueError("CQLTrainer did not reach {} reward from expert "
                         "offline data!".format(min_reward))

    # If you would like to query CQL's learnt Q-function for arbitrary
    # (cont.) actions, do the following:
    obs_batch = torch.from_numpy(np.random.random(size=(5, 3)))
    action_batch = torch.from_numpy(np.random.random(size=(5, 1)))

    cql_model = trainer.get_policy().model
    q_values = cql_model.get_q_values([obs_batch], [action_batch])
    # If you are using the "twin_q", there'll be 2 Q-networks and
    # we usually consider the min of the 2 outputs, like so:
    twin_q_values = cql_model.get_twin_q_values([obs_batch], [action_batch])
    final_q_values = torch.min(q_values, twin_q_values)
    print(final_q_values)

    trainer.stop()
