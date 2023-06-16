from ray import tune, air
from ray.rllib.algorithms.ppo import PPOConfig
import argparse


def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--num-iters", "-n", type=int, default=5000, help="Number of iterations")
    parser.add_argument("--backend", type=str, default="cudagraphs", help="torch dynamo backend")
    parser.add_argument("--mode", type=str, default=None, help="torch dynamo mode")

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
            }
        )
        .rl_module(_enable_rl_module_api=True)
        .training( 
            lambda_=0.95,
            kl_coeff=0.5,
            vf_clip_param=10.0,
            entropy_coeff=0.01,
            train_batch_size=16000,
            sgd_minibatch_size=2000,
            num_sgd_iter=10,
            vf_loss_coeff=0.01,
            clip_param=0.1,
            lr=0.0001,
            grad_clip=100,
            grad_clip_by="global_norm",
            _enable_learner_api=True,
            # model={
            #     "vf_share_layers": True,
            #     "conv_filters": [[16, 4, 2], [32, 4, 2], [64, 4, 2], [128, 4, 2]],
            #     "conv_activation": "relu",
            #     "post_fcnet_hiddens": [256],
            # },
        )
        .rollouts(
            num_rollout_workers=64, 
            num_envs_per_worker=1,
            batch_mode="truncate_episodes",
            rollout_fragment_length="auto",
            create_env_on_local_worker=True
        )
        .framework(
            "torch",
            torch_compile_worker=tune.grid_search([True, False]), 
            torch_compile_worker_dynamo_backend=pargs.backend,
            torch_compile_worker_dynamo_mode=pargs.mode,
            # torch_compile_worker_dynamo_mode="max-autotune"
        )
        .resources(
            num_learner_workers=8,
            num_gpus_per_learner_worker=1,
        )
    )

    tuner = tune.Tuner(
        "PPO",
        run_config=air.RunConfig(
            stop={"training_iteration": pargs.num_iters},
        ),
        param_space=config,
    )

    results = tuner.fit()

    compiled_throughput = results[0].metrics["num_env_steps_sampled_throughput_per_sec"]
    eager_throughput = results[1].metrics["num_env_steps_sampled_throughput_per_sec"]
    print(f"Speed up (%): {compiled_throughput / eager_throughput - 1}")


# burn_in = 0
# num_iter = 100
# assert num_iter > burn_in

# env = wrap_deepmind(gym.make("GymV26Environment-v0", env_id="ALE/Breakout-v5"))
# results = []
# stats = []
# for compile in [True, False]:
#     config.framework(torch_compile_worker=compile)
#     algo = config.build()
#     # policy = algo.get_policy()
#     local_worker = algo.workers.local_worker()
#     sampler = local_worker.sampler
#     env_runner = sampler._env_runner_obj
#     policy = env_runner._worker.policy_map["default_policy"]
#     throughtputs = []
#     for iter in tqdm.tqdm(range(num_iter)):
#         batch = get_ppo_batch_for_env(env, batch_size=1)
#         batch = convert_to_torch_tensor(batch, device="cpu")
#         s = time.time()
#         # policy.compute_actions_from_input_dict(batch)
#         sampler.get_data()
#         if iter > burn_in:
#             throughtputs.append(1 / (time.time() - s))
    
#     print("Compile: ", compile)
#     print("Median throughtput: ", np.median(throughtputs))
#     print("Max throughtput: ", max(throughtputs))
#     print("Min throughtput: ", min(throughtputs))
#     stats.append(env_runner._perf_stats.get())
#     results.append(throughtputs)

# pprint.pprint(stats)

# upper_limit = max([max(r) for r in results])
# fig, axes = plt.subplots(nrows=1, ncols=2, figsize=(10, 3))
# axes[0].plot(results[0])
# axes[0].set_title(f'compiled iter / sec')
# axes[0].set_ylim(0, upper_limit)
# axes[1].plot(results[1])
# axes[1].set_title(f'non-compiled iter / sec')
# axes[1].set_ylim(0, upper_limit)
# fig.tight_layout()
# plt.savefig("throughputs.png")
# plt.clf()

if __name__ == "__main__":
    pargs = _parse_args()
    main(pargs)
