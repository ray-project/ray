import argparse

from ray import tune, air
from ray.rllib.algorithms.ppo import PPOConfig

# Note:
# To run this benchmark you need to have a ray cluster of at least
# 129 CPUs (2x64 + 1) and 2 GPUs
# For smoke test, you can use 3 CPUs


def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--num-iters", "-n", type=int, default=10, help="Number of iterations"
    )
    parser.add_argument(
        "--backend", type=str, default="onnxrt", help="torch dynamo backend"
    )
    parser.add_argument("--mode", type=str, default=None, help="torch dynamo mode")
    parser.add_argument("--smoke-test", action="store_true", help="smoke test")

    return parser.parse_args()


def main(pargs):

    config = (
        PPOConfig()
        .environment(
            "ALE/Breakout-v5",
            clip_rewards=True,
            env_config={
                "frameskip": 1,
                "full_action_space": False,
                "repeat_action_probability": 0.0,
            },
        )
        .training(
            lambda_=0.95,
            kl_coeff=0.5,
            vf_clip_param=10.0,
            entropy_coeff=0.01,
            train_batch_size=32 if pargs.smoke_test else 16000,
            sgd_minibatch_size=1 if pargs.smoke_test else 2000,
            num_sgd_iter=1 if pargs.smoke_test else 10,
            vf_loss_coeff=0.01,
            clip_param=0.1,
            lr=0.0001,
            grad_clip=100,
            grad_clip_by="global_norm",
        )
        .rollouts(
            num_rollout_workers=1 if pargs.smoke_test else 64,
            num_envs_per_worker=1,
            batch_mode="truncate_episodes",
            rollout_fragment_length="auto",
            create_env_on_local_worker=True,
        )
        .framework(
            "torch",
            torch_compile_worker=tune.grid_search([True, False]),
            torch_compile_worker_dynamo_backend=pargs.backend,
            torch_compile_worker_dynamo_mode=pargs.mode,
        )
        .resources(
            num_learner_workers=1,
            num_gpus_per_learner_worker=0 if pargs.smoke_test else 1,
        )
    )

    tuner = tune.Tuner(
        "PPO",
        run_config=air.RunConfig(
            stop={"training_iteration": 1 if pargs.smoke_test else pargs.num_iters},
        ),
        param_space=config,
    )

    results = tuner.fit()

    compiled_throughput = results[0].metrics["num_env_steps_sampled_throughput_per_sec"]
    eager_throughput = results[1].metrics["num_env_steps_sampled_throughput_per_sec"]
    print(f"Speed up (%): {100 * (compiled_throughput / eager_throughput - 1)}")


if __name__ == "__main__":
    main(_parse_args())
