import ray.tune as tune
from ray.rllib.algorithms.ppo import PPOConfig
import ray
import torch

config = PPOConfig().rl_module(_enable_rl_module_api=True).training(_enable_learner_api=True)
config.environment("ALE/Breakout-v5", clip_rewards=True,)
config.training( 
    train_batch_size= 5000,
    sgd_minibatch_size= 500,
    num_sgd_iter=10,
    vf_loss_coeff=0.01,
    lambda_=0.95,
    kl_coeff=0.5,
    clip_param=0.1,
    )
config.rollouts(
    num_rollout_workers=7, 
    num_envs_per_worker=16,
    batch_mode="truncate_episodes",
    )

from torch import _dynamo
torch._dynamo.allow_in_graph(torch.distributions.kl.kl_divergence)
torch._dynamo.disallow_in_graph(torch.distributions.kl.kl_divergence)

EVALUATE_TOGETHER = False

if EVALUATE_TOGETHER:
    config.framework(
        torch_compile_worker=tune.grid_search([True, False]), 
        torch_compile_learner=tune.grid_search([True, False]),
    )
    config.resources(num_gpus_per_learner_worker=1)

    run_config= ray.air.RunConfig(
        stop={"training_iteration": 10},
        )
    tune_config = tune.TuneConfig(num_samples=1)

    tuner = tune.Tuner(
        "PPO",
        run_config=run_config,
        tune_config=tune_config,
        param_space=config,
    )

    tuner.fit()
else:
    for cworker in [True, False]:
        for clearner in [True, False]:

            config.framework(
                torch_compile_worker=cworker, 
                torch_compile_learner=clearner,
            )
            config.resources(num_gpus_per_learner_worker=1)

            run_config= ray.air.RunConfig(
                stop={"training_iteration": 15},
                name=f"cworker{cworker}-clearner{clearner}",
                )
            tune_config = tune.TuneConfig(num_samples=1)

            tuner = tune.Tuner(
                "PPO",
                run_config=run_config,
                tune_config=tune_config,
                param_space=config,
            )

            tuner.fit()