from gymnasium.wrappers import TimeLimit
from rllib_maml.maml import MAML, MAMLConfig

import ray
from ray import air, tune
from ray.rllib.examples.env.cartpole_mass import CartPoleMassEnv
from ray.tune.registry import register_env

if __name__ == "__main__":
    ray.init()
    register_env(
        "cartpole",
        lambda env_cfg: TimeLimit(CartPoleMassEnv(), max_episode_steps=200),
    )

    rollout_fragment_length = 32

    config = (
        MAMLConfig()
        .rollouts(
            num_rollout_workers=4, rollout_fragment_length=rollout_fragment_length
        )
        .framework("torch")
        .environment("cartpole", clip_actions=False)
        .training(
            inner_adaptation_steps=1,
            maml_optimizer_steps=5,
            gamma=0.99,
            lambda_=1.0,
            lr=0.001,
            vf_loss_coeff=0.5,
            inner_lr=0.03,
            use_meta_env=False,
            clip_param=0.3,
            kl_target=0.01,
            kl_coeff=0.001,
            model=dict(fcnet_hiddens=[64, 64]),
            train_batch_size=rollout_fragment_length,
        )
    )

    num_iterations = 100

    tuner = tune.Tuner(
        MAML,
        param_space=config.to_dict(),
        run_config=air.RunConfig(
            stop={"training_iteration": num_iterations},
            failure_config=air.FailureConfig(fail_fast="raise"),
        ),
    )
    results = tuner.fit()
