import ray
from ray import air, tune
from ray.rllib.algorithms.callbacks import DefaultCallbacks, make_multi_callbacks
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.examples.rlhf.ppo_ft.rlhf_env import RLHFEnv
from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec

from ray.rllib.examples.rlhf.ppo_ft.rlhf_ppo_module import RLHFPPOTorchRLModule
from ray.rllib.examples.rlhf.ppo_ft.ppo_rlhf import PPORLHF

ray.init(local_mode=True)

class ValueFunctionInitializerCallback(DefaultCallbacks):

    def on_algorithm_init(self, *, algorithm, **kwargs) -> None:
        learner_group = algorithm.learner_group

        # TODO (Kourosh): We should allow users to run foreach on each learner within 
        # the group for example to load initialize the value function with RM weights.
        # learner_group.foreach(
        #   lambda learner, **kwargs: 
        #       learner.module.value_fn.load_state_dict(rm.state_dict())
        # )



env_config = {
    "tokenizer_path": "gpt2",
    "sft_model_path": "gpt2",
    "prompt_dataset_path": "yizhongw/self_instruct",
    "prompt_dataset_split": "train",
    "kl_coeff": 0.2,
    "max_generation_length": 50,
}

env_creator = lambda config: RLHFEnv(config)
tune.register_env("RLHFEnv", env_creator)


# example env creation
env = RLHFEnv(env_config)

config = (
    PPOConfig(algo_class=PPORLHF)
    .framework("torch")
    .environment(
        "RLHFEnv", 
        env_config=env_config,
        # observation_space=env.observation_space,
        # action_space=env.action_space,
        disable_env_checking=True,
    )
    .rl_module(
        _enable_rl_module_api=True,
        rl_module_spec=SingleAgentRLModuleSpec(RLHFPPOTorchRLModule),
    )
    .training(
        num_sgd_iter=1,
        sgd_minibatch_size=1,
        train_batch_size=1,
        _enable_learner_api=True
    )
    .rollouts(
        num_rollout_workers=0
    )
    .experimental(
        _disable_preprocessor_api=True,
        _disable_initialize_loss_from_dummy_batch=True,
    )
    .callbacks(
        callbacks_class=make_multi_callbacks([
            ValueFunctionInitializerCallback,
        ])
    )
)

algo = config.build()
algo.train()