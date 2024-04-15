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



"""
from ray import train, tune
from ray.rllib.algorithms.ppo import PPO, PPOConfig
from ray.rllib.env.single_agent_env_runner import SingleAgentEnvRunner


def my_experiment(config):
    iterations = config.pop("train-iterations", 10)

    config = PPOConfig().update_from_dict(config).environment("CartPole-v1")

    # Train for n iterations with high LR.
    config.lr = 0.01
    agent1 = config.build()
    for _ in range(iterations):
        result = agent1.train()
        result["phase"] = 1
        train.report(result)
        phase1_time = result["timesteps_total"]
    state = agent1.save()
    agent1.stop()

    # Train for n iterations with low LR
    config.lr = 0.0001
    agent2 = config.build()
    agent2.restore(state)
    for _ in range(iterations):
        result = agent2.train()
        result["phase"] = 2
        result["timesteps_total"] += phase1_time  # keep time moving forward
        train.report(result)
    agent2.stop()

    # Manual Eval
    config["num_workers"] = 0
    eval_algo = ppo.PPO(config=config)
    eval_algo.restore(checkpoint)
    env = eval_algo.workers.local_worker().env

    obs, info = env.reset()
    done = False
    eval_results = {"eval_reward": 0, "eval_eps_length": 0}
    while not done:
        action = eval_algo.compute_single_action(obs)
        next_obs, reward, done, truncated, info = env.step(action)
        eval_results["eval_reward"] += reward
        eval_results["eval_eps_length"] += 1
    results = {**train_results, **eval_results}
    train.report(results)


if __name__ == "__main__":

    base_config = (
        PPOConfig()
        .experimental(_enable_new_api_stack=True)
        .environment("CartPole-v1")
        .rollouts(
            num_rollout_workers=0,
            env_runner_cls=SingleAgentEnvRunner,
        )
    )
    # Convert to a plain dict for Tune. Note that this is usually not needed, you can
    # pass into the below Tune Tuner any instantiated RLlib AlgorithmConfig object.
    # However, for demonstration purposes, we show here how you can add other, arbitrary
    # keys to the plain config dict and then pass these keys to your custom experiment
    # function.
    config_dict = base_config.to_dict()

    # Set a Special flag signalling `my_train_fn` how many iters to do.
    config_dict["train-iterations"] = 2

    training_function = tune.with_resources(
        my_experiment,
        resources=base_config.algo_class.default_resource_request(base_config),
    )

    tuner = tune.Tuner(
        training_function,
        # Pass in your config dict.
        param_space=config_dict,
    )
    tuner.fit()
