from ray import tune

from ray.rllib.algorithms.ppo.ppo import PPOConfig

from ray.rllib.algorithms.ppo.tests.test_envs import (
    OneActionZeroObsOneRewardEnv,
    OneActionRandomObsTwoRewardEnv,
    OneActionTwoObsOneRewardEnv,
    TwoActionOneObsTwoRewardEnv,
    TwoActionRandomObsTwoRewardEnv,
)

tune.register_env(
    "test_value_function_env", lambda ctx: OneActionZeroObsOneRewardEnv(ctx)
)
tune.register_env(
    "test_value_function_backprop_env", lambda ctx: OneActionRandomObsTwoRewardEnv(ctx)
)
tune.register_env(
    "test_value_function_discounting_env", lambda ctx: OneActionTwoObsOneRewardEnv(ctx)
)
tune.register_env(
    "test_policy_action_dependent_reward_env",
    lambda ctx: TwoActionOneObsTwoRewardEnv(ctx),
)
tune.register_env(
    "test_policy_action_dependent_reward_and_obs_env",
    lambda ctx: TwoActionRandomObsTwoRewardEnv(ctx),
)


envs = [
    # "test_value_function_env",
    # "test_value_function_backprop_env",
    # "test_value_function_discounting_env",
    "test_policy_action_dependent_reward_env",
    # "test_policy_action_dependent_reward_and_obs_env",
]


def return_env_runner_cls(cls: str = "SingleAgentEnvRunner"):
    if cls == "SingleAgentEnvRunner":
        from ray.rllib.env.single_agent_env_runner import SingleAgentEnvRunner

        return SingleAgentEnvRunner
    else:
        from ray.rllib.evaluation.rollout_worker import RolloutWorker

        return RolloutWorker


config = (
    PPOConfig()
    .framework(
        framework="tf2",
        eager_tracing=False,
    )
    .rollouts(
        rollout_fragment_length=100,
        env_runner_cls=return_env_runner_cls(),
        num_envs_per_worker=2,
    )
    .rl_module(
        _enable_rl_module_api=True,
    )
    .evaluation(
        enable_async_evaluation=True,
    )
    .training(
        lr=0.0005,
        vf_loss_coeff=1.0,
        train_batch_size=800,
        sgd_minibatch_size=25,
        num_sgd_iter=20,
        _enable_learner_api=True,
        model={
            "fcnet_hiddens": [64, 64],
        },
    )
    .debugging(seed=0)
)

# import ray
# ray.init(local_mode=True)
training_iterations = 5
for env in envs:
    if env == "test_value_function_discounting_env":
        training_iterations = 10
    else:
        training_iterations = 20

    config.environment(
        env=env,
    )
    algo = config.build()

    for i in range(training_iterations):
        results = algo.train()
        # print(results)

    module = algo.workers.local_worker().module
    worker = algo.workers.local_worker()
    print("-" * len(env))
    print(env)
    print("=" * len(env))

    env_entity = worker.env.envs[0]
    for obs in env_entity.check_states:
        input_batch = {
            "state_in": {},
            "obs": worker._convert_from_numpy(obs),
        }

        fwd_out = module.forward_exploration(input_batch)
        value = fwd_out["vf_preds"]
        action_dist_inputs = fwd_out["action_dist_inputs"]
        action_dist_cls = module.get_exploration_action_dist_cls()
        action_dist = action_dist_cls.from_logits(action_dist_inputs)
        action = action_dist.sample().numpy()
        env_entity.assertion(obs=obs, value=value, action=action)


# chkpt_dict = {}
# import ray
# ray.init(local_mode=True)
# for env in envs:
#     config.environment(
#         env=env,
#     )

#     if "test_policy_action_dependent_reward_env" in env:
#         stop = {"training_iteration": 15}
#     elif "test_policy_action_dependent_reward_and_obs_env" in env:
#         stop = {"training_iteration": 10}
#     elif "value_and_cost" in env:
#         config.training(
#             cost_surr_coeff=0.0,
#             cost_limit=25.0,
#             cf_loss_coeff=1.0,
#             cf_clip_param=float("inf"),
#             kl_early_stop=False,
#             use_l2_norm=False,
#         )
#         stop = {"training_iteration": 5}
#     elif "reward_and_cost" in env:
#         config.training(
#             lr=0.0005,
#             cost_surr_coeff=1.0,
#             cost_limit=0.0,
#             cf_loss_coeff=1.0,
#             vf_loss_coeff=1.0,
#             cf_clip_param=10,  # float("inf"),
#             kl_early_stop=False,
#             use_l2_norm=False,
#             cost_loss="experimental",
#         )
#         if "test_policy_action_dependent_reward_and_cost_and_obs_env":
#             config.training(
#                 use_kl_loss=True,
#                 num_sgd_iter=10,
#                 clip_param=0.1,
#                 cf_clip_param=10,
#                 cost_limit=0.0,
#             )
#         stop = {"training_iteration": 35}
#     else:
#         stop = {"training_iteration": 5}

#     tuner = tune.Tuner(
#         "PPO",
#         param_space=config,
#         run_config=air.RunConfig(
#             stop=stop,
#             name="safe_rllib_" + Path(__file__).name.split(".")[0],
#         ),
#     )

#     result = tuner.fit()
#     chkpt_dict[env] = result.get_best_result().checkpoint

# env_id = "env_1"
# agent_id = "agent_1"
# for env, chkpt in chkpt_dict.items():
#     # Load artifacts.
#     algo = PPO.from_checkpoint(chkpt)
#     env_entity = algo.env_creator(algo.config)

# module = algo.workers.local_worker().module
# worker = algo.workers.local_worker()
# print("-" * len(env))
# print(env)
# print("=" * len(env))

# for obs in env_entity.check_states:
#     input_batch =  {
#         "state_in": {},
#         "obs": worker._convert_from_numpy(obs),
#     }

#     fwd_out = module.forward_exploration(input_batch)
#     value = fwd_out["vf_preds"]
#     action = fwd_out["actions"]
#     env_entity.assertion(obs=obs, value=value, action=action)
