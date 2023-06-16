from ray import tune, air
from ray.rllib.algorithms.ppo import PPOConfig


config = (
    PPOConfig()
    .environment("ALE/Breakout-v5", clip_rewards=True)
    .rl_module(_enable_rl_module_api=True)
    .training( 
        train_batch_size= 5000,
        sgd_minibatch_size= 500,
        num_sgd_iter=10,
        vf_loss_coeff=0.01,
        lambda_=0.95,
        kl_coeff=0.5,
        clip_param=0.1,
        _enable_learner_api=True,
    )
    .rollouts(
        num_rollout_workers=0, 
        # num_envs_per_worker=1,
        batch_mode="truncate_episodes",
    )
    .framework(
        torch_compile_worker=tune.grid_search([True, False]), 
        # torch_compile_worker=True, 
        torch_compile_worker_dynamo_backend="onnxrt",
        torch_compile_worker_dynamo_mode="reduce-overhead"
    )
)
# config.build()
tuner = tune.Tuner(
    "PPO",
    run_config=air.RunConfig(
        stop={"training_iteration": 10},
    ),
    param_space=config,
)

tuner.fit()