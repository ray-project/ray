"""Example of a custom Ray Tune experiment wrapping an RLlib Algorithm.

You should only use such a customized workflow if the following conditions apply:
- You know exactly what you are doing :)
- Configuring an existing RLlib Algorithm (e.g. PPO) via its AlgorithmConfig
is not sufficient and doesn't allow you to shape the Algorithm into behaving the way
you'd like. Note that for complex, custom evaluation procedures there are many
AlgorithmConfig options one can use (for more details, see:
https://github.com/ray-project/ray/blob/master/rllib/examples/evaluation/custom_evaluation.py).  # noqa
- Subclassing an RLlib Algorithm class and overriding the new class' `training_step`
method is not sufficient and doesn't allow you to define the algorithm's execution
logic the way you'd like. See an example here on how to customize the algorithm's
`training_step()` method:
https://github.com/ray-project/ray/blob/master/rllib/examples/algorithm/custom_training_step_on_and_off_policy_combined.py  # noqa


How to run this script
----------------------
`python [script file name].py`


Results to expect
-----------------
You should see the following output (at the end of the experiment) in your console:

╭───────────────────────────────────────────────────────────────────────────────────────
│ Trial name                              status         iter     total time (s)      ts
├───────────────────────────────────────────────────────────────────────────────────────
│ my_experiment_CartPole-v1_77083_00000   TERMINATED       10            36.7799   60000
╰───────────────────────────────────────────────────────────────────────────────────────
╭───────────────────────────────────────────────────────╮
│     reward    episode_len_mean     episodes_this_iter │
├───────────────────────────────────────────────────────┤
│    254.821             254.821                     12 │
╰───────────────────────────────────────────────────────╯
evaluation episode returns=[500.0, 500.0, 500.0]

Note that evaluation results (on the CartPole-v1 env) should be close to perfect
(episode return of ~500.0) as we are acting greedily inside the evaluation procedure.
"""
from typing import Dict

import numpy as np
from ray import train, tune
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.metrics import NUM_ENV_STEPS_SAMPLED_LIFETIME

torch, _ = try_import_torch()


def my_experiment(config: Dict):

    # Extract the number of iterations to run from the config.
    train_iterations = config.pop("train-iterations", 2)
    eval_episodes_to_do = config.pop("eval-episodes", 1)

    config = (
        PPOConfig()
        .update_from_dict(config)
        .api_stack(enable_rl_module_and_learner=True)
        .environment("CartPole-v1")
    )

    # Train for n iterations with high LR.
    config.training(lr=0.001)
    algo_high_lr = config.build()
    for _ in range(train_iterations):
        train_results = algo_high_lr.train()
        # Add the phase to the result dict.
        train_results["phase"] = 1
        train.report(train_results)
        phase_high_lr_time = train_results[NUM_ENV_STEPS_SAMPLED_LIFETIME]
    checkpoint_training_high_lr = algo_high_lr.save()
    algo_high_lr.stop()

    # Train for n iterations with low LR.
    config.training(lr=0.00001)
    algo_low_lr = config.build()
    # Load state from the high-lr algo into this one.
    algo_low_lr.restore(checkpoint_training_high_lr)
    for _ in range(train_iterations):
        train_results = algo_low_lr.train()
        # Add the phase to the result dict.
        train_results["phase"] = 2
        # keep time moving forward
        train_results[NUM_ENV_STEPS_SAMPLED_LIFETIME] += phase_high_lr_time
        train.report(train_results)

    checkpoint_training_low_lr = algo_low_lr.save()
    algo_low_lr.stop()

    # After training, run a manual evaluation procedure.

    # Set the number of EnvRunners for collecting training data to 0 (local
    # worker only).
    config.env_runners(num_env_runners=0)

    eval_algo = config.build()
    # Load state from the low-lr algo into this one.
    eval_algo.restore(checkpoint_training_low_lr)
    # The algo's local worker (SingleAgentEnvRunner) that holds a
    # gym.vector.Env object and an RLModule for computing actions.
    local_env_runner = eval_algo.env_runner
    # Extract the gymnasium env object from the created algo (its local
    # SingleAgentEnvRunner worker). Note that the env in this single-agent
    # case is a gymnasium vector env and that we get its first sub-env here.
    env = local_env_runner.env.unwrapped.envs[0]

    # The local worker (SingleAgentEnvRunner)
    rl_module = local_env_runner.module

    # Run a very simple env loop and add up rewards over a single episode.
    obs, infos = env.reset()
    episode_returns = []
    episode_lengths = []
    sum_rewards = length = 0
    num_episodes = 0
    while num_episodes < eval_episodes_to_do:
        # Call the RLModule's `forward_inference()` method to compute an
        # action.
        rl_module_out = rl_module.forward_inference(
            {
                "obs": torch.from_numpy(np.expand_dims(obs, 0)),  # <- add B=1
            }
        )
        action_logits = rl_module_out["action_dist_inputs"][0]  # <- remove B=1
        action = np.argmax(action_logits.detach().cpu().numpy())  # act greedily

        # Step the env.
        obs, reward, terminated, truncated, info = env.step(action)

        # Acculumate stats and reset the env, if necessary.
        sum_rewards += reward
        length += 1
        if terminated or truncated:
            num_episodes += 1
            episode_returns.append(sum_rewards)
            episode_lengths.append(length)
            sum_rewards = length = 0
            obs, infos = env.reset()

    # Compile evaluation results.
    eval_results = {
        "eval_returns": episode_returns,
        "eval_episode_lengths": episode_lengths,
    }
    # Combine the most recent training results with the just collected
    # evaluation results.
    results = {**train_results, **eval_results}
    # Report everything.
    train.report(results)


if __name__ == "__main__":
    base_config = PPOConfig().environment("CartPole-v1").env_runners(num_env_runners=0)
    # Convert to a plain dict for Tune. Note that this is usually not needed, you can
    # pass into the below Tune Tuner any instantiated RLlib AlgorithmConfig object.
    # However, for demonstration purposes, we show here how you can add other, arbitrary
    # keys to the plain config dict and then pass these keys to your custom experiment
    # function.
    config_dict = base_config.to_dict()

    # Set a Special flag signalling `my_experiment` how many training steps to
    # perform on each: the high learning rate and low learning rate.
    config_dict["train-iterations"] = 5
    # Set a Special flag signalling `my_experiment` how many episodes to evaluate for.
    config_dict["eval-episodes"] = 3

    training_function = tune.with_resources(
        my_experiment,
        resources=base_config.algo_class.default_resource_request(base_config),
    )

    tuner = tune.Tuner(
        training_function,
        # Pass in your config dict.
        param_space=config_dict,
    )
    results = tuner.fit()
    best_results = results.get_best_result()

    print(f"evaluation episode returns={best_results.metrics['eval_returns']}")
