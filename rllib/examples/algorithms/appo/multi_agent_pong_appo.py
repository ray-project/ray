import random

import gymnasium as gym

from ray.rllib.algorithms.appo import APPOConfig
from ray.rllib.connectors.env_to_module.frame_stacking import FrameStackingEnvToModule
from ray.rllib.connectors.learner.frame_stacking import FrameStackingLearner
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.core.rl_module.multi_rl_module import MultiRLModuleSpec
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.env.multi_agent_env import make_multi_agent
from ray.rllib.env.wrappers.atari_wrappers import wrap_atari_for_new_api_stack
from ray.rllib.examples.rl_modules.classes.random_rlm import RandomRLModule
from ray.rllib.utils.test_utils import add_rllib_example_script_args

parser = add_rllib_example_script_args(
    default_reward=0.0,
    default_timesteps=20000000,
    default_iters=400,
)
parser.set_defaults(
    env="ale_py:ALE/Pong-v5",
    num_agents=2,
)
args = parser.parse_args()


def _make_env_to_module_connector(env, spaces, device):
    return FrameStackingEnvToModule(num_frames=4, multi_agent=True)


def _make_learner_connector(input_observation_space, input_action_space):
    return FrameStackingLearner(num_frames=4, multi_agent=True)


def _env_creator(cfg):
    return wrap_atari_for_new_api_stack(
        gym.make(args.env, **cfg, **{"render_mode": "rgb_array"}),
        dim=64,
        framestack=None,
    )


MultiAgentPong = make_multi_agent(_env_creator)
NUM_POLICIES = 5
main_spec = RLModuleSpec(
    model_config=DefaultModelConfig(
        vf_share_layers=True,
        conv_filters=[(16, 4, 2), (32, 4, 2), (64, 4, 2), (128, 4, 2)],
        conv_activation="relu",
        head_fcnet_hiddens=[256],
    ),
)


config = (
    APPOConfig()
    .environment(
        MultiAgentPong,
        env_config={
            "num_agents": args.num_agents,
            # Make analogous to old v4 + NoFrameskip.
            "frameskip": 1,
            "full_action_space": False,
            "repeat_action_probability": 0.0,
        },
        clip_rewards=True,
    )
    .env_runners(
        env_to_module_connector=_make_env_to_module_connector,
    )
    .learners(
        num_aggregator_actors_per_learner=2,
    )
    .training(
        learner_connector=_make_learner_connector,
        train_batch_size_per_learner=500,
        target_network_update_freq=2,
        lr=0.0005 * ((args.num_learners or 1) ** 0.5),
        vf_loss_coeff=1.0,
        entropy_coeff=[[0, 0.01], [3000000, 0.0]],  # <- crucial parameter to finetune
        # Only update connector states and model weights every n training_step calls.
        broadcast_interval=5,
        # learner_queue_size=1,
        circular_buffer_num_batches=4,
        circular_buffer_iterations_per_batch=2,
    )
    .rl_module(
        rl_module_spec=MultiRLModuleSpec(
            rl_module_specs=(
                {f"p{i}": main_spec for i in range(NUM_POLICIES)}
                | {"random": RLModuleSpec(module_class=RandomRLModule)}
            ),
        ),
    )
    .multi_agent(
        policies={f"p{i}" for i in range(NUM_POLICIES)} | {"random"},
        policy_mapping_fn=lambda aid, eps, **kw: (
            random.choice([f"p{i}" for i in range(NUM_POLICIES)] + ["random"])
        ),
        policies_to_train=[f"p{i}" for i in range(NUM_POLICIES)],
    )
)


if __name__ == "__main__":
    from ray.rllib.utils.test_utils import run_rllib_example_script_experiment

    run_rllib_example_script_experiment(config, args)
