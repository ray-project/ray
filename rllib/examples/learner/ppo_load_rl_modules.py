import argparse
import gymnasium as gym
import shutil
import tempfile

import ray
from ray import air, tune
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.algorithms.ppo.ppo_catalog import PPOCatalog
from ray.rllib.algorithms.ppo.tf.ppo_tf_rl_module import PPOTfRLModule
from ray.rllib.algorithms.ppo.torch.ppo_torch_rl_module import PPOTorchRLModule
from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec


def _parse_args():

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--framework",
        choices=["tf2", "torch"],  # tf will be deprecated with the new Learner stack
        default="torch",
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()

    ray.init()

    # Create a module to load and save it to a checkpoint for testing purposes
    # (this is not necessary in a real use case)
    # In a real case you would just load the checkpoint from a rllib training run
    # where you had enabled checkpointing, the learner api and the rl module api
    module_class = PPOTfRLModule if args.framework == "tf2" else PPOTorchRLModule
    env = gym.make("CartPole-v1")
    module_to_load = SingleAgentRLModuleSpec(
        module_class=module_class,
        model_config_dict={"fcnet_hiddens": [32]},
        catalog_class=PPOCatalog,
        observation_space=env.observation_space,
        action_space=env.action_space,
    ).build()

    CHECKPOINT_DIR = tempfile.mkdtemp()
    module_to_load.save_to_checkpoint(CHECKPOINT_DIR)

    # Create a module spec to load the checkpoint
    module_to_load_spec = SingleAgentRLModuleSpec(
        module_class=module_class,
        model_config_dict={"fcnet_hiddens": [32]},
        catalog_class=PPOCatalog,
        load_state_path=CHECKPOINT_DIR,
    )

    # train a PPO algorithm with the loaded module
    config = (
        PPOConfig()
        .framework(args.framework)
        .training(_enable_learner_api=True)
        .rl_module(_enable_rl_module_api=True, rl_module_spec=module_to_load_spec)
        .environment("CartPole-v1")
    )

    tuner = tune.Tuner(
        "PPO",
        param_space=config.to_dict(),
        run_config=air.RunConfig(
            stop={"training_iteration": 1},
            failure_config=air.FailureConfig(fail_fast="raise"),
        ),
    )
    tuner.fit()
    shutil.rmtree(CHECKPOINT_DIR)
