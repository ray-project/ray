from ray import tune, air
from ray.rllib.algorithms.ppo import PPOConfig


config = (
    PPOConfig()
    .environment(
        "ALE/Breakout-v5", 
        clip_rewards=True,
        env_config={
            "frame_skip": 1,
            "full_action_space": False,
            "repeat_action_probability": 0.0,
        }
    )
    .rl_module(_enable_rl_module_api=True)
    .training( 
        train_batch_size=5000,
        sgd_minibatch_size=500,
        rollout_fragment_length="auto",
        num_sgd_iter=10,
        vf_loss_coeff=0.01,
        vf_clip_param=10.0,
        entropy_coeff=0.01,
        lambda_=0.95,
        kl_coeff=0.5,
        clip_param=0.1,
        lr=0.0001,
        grad_clip=100,
        _enable_learner_api=True,
        model={"vf_share_layers": True},
    )
    .rollouts(
        num_rollout_workers=16, 
        num_envs_per_worker=1,
        batch_mode="truncate_episodes",
    )
    .framework(
        "torch",
        torch_compile_worker=tune.grid_search([True, False]), 
        # torch_compile_worker=False, 
        torch_compile_worker_dynamo_backend="onnxrt",
        torch_compile_worker_dynamo_mode="reduce-overhead"
    )
    .resources(
        num_learner_workers=4,
        num_gpus_per_learner_worker=1,
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