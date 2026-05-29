from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.core import DEFAULT_POLICY_ID
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.examples.rl_modules.classes.modelv2_to_rlm import ModelV2ToRLModule
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
)

if __name__ == "__main__":
    # Configure an old stack default ModelV2.
    config_old_stack = (
        PPOConfig()
        .api_stack(
            enable_env_runner_and_connector_v2=False,
            enable_rl_module_and_learner=False,
        )
        .environment("CartPole-v1")
        .training(
            lr=0.0003,
            num_sgd_iter=6,
            vf_loss_coeff=0.01,
            # Change the ModelV2 settings a bit.
            model={
                "fcnet_hiddens": [32],
                "fcnet_activation": "linear",
                "use_lstm": True,
                "vf_share_layers": True,
            },
        )
    )

    # Training with the (configured and wrapped) ModelV2.

    # We change the original (old API stack) `config` into a new API stack one:
    config_new_stack = (
        config_old_stack.copy(copy_frozen=False)
        .api_stack(
            enable_rl_module_and_learner=True,
            enable_env_runner_and_connector_v2=True,
        )
        .rl_module(
            rl_module_spec=RLModuleSpec(
                module_class=ModelV2ToRLModule,
                model_config={
                    "policy_id": DEFAULT_POLICY_ID,
                    "old_api_stack_algo_config": config_old_stack,
                    "max_seq_len": 20,
                },
            ),
        )
    )

    # Build the new stack algo.
    algo_new_stack = config_new_stack.build()

    # Train until a higher return.
    min_return_new_stack = 350.0
    results = None
    passed = False
    for i in range(100):
        results = algo_new_stack.train()
        print(results)
        if results[ENV_RUNNER_RESULTS][EPISODE_RETURN_MEAN] >= min_return_new_stack:
            print(
                f"Reached episode return of {min_return_new_stack} -> stopping "
                "new API stack training."
            )
            passed = True
            break

    if not passed:
        raise ValueError(
            "Continuing training on the new stack did not succeed! Last return: "
            f"{results[ENV_RUNNER_RESULTS][EPISODE_RETURN_MEAN]}"
        )
