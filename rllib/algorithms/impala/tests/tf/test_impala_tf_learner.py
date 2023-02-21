import unittest
import gymnasium as gym

from ray.rllib.algorithms.ppo.tf.ppo_tf_rl_module import PPOTfRLModule
from ray.rllib.algorithms.impala.tf.impala_tf_learner import ImpalaTfLearner, ImpalaHPs
from ray.rllib.core.testing.utils import do_rollout_single_agent
from ray.rllib.execution.buffers.mixin_replay_buffer import MixInMultiAgentReplayBuffer
from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec


class TestImpalaTfLearner(unittest.TestCase):
    def test_impala_tf_learner_convergence(self):
        env = gym.make("CartPole-v1")
        episode_length = 500
        model_config = {
            "hidden_dim": 32,
            "fcnet_activation": "relu",
            "fcnet_hiddens": [32],
            "vf_share_layers": False,
            "use_lstm": False,
        }
        module = PPOTfRLModule.from_model_config(
            env.observation_space, env.action_space, model_config=model_config
        ).as_multi_agent()
        learner = ImpalaTfLearner(
            module_spec=SingleAgentRLModuleSpec(
                module_class=PPOTfRLModule,
                observation_space=env.observation_space,
                action_space=env.action_space,
                model_config=model_config,
            ),
            learner_hyperparameters=ImpalaHPs(
                rollout_frag_or_episode_len=episode_length
            ),
            optimizer_config={"lr": 5e-4, "grad_clip": 40.0},
        )
        learner.build()
        module.set_state(learner.get_state()["module_state"])
        local_mixin_buffer = MixInMultiAgentReplayBuffer(
            capacity=100,
            replay_ratio=0.25,
        )
        for i in range(500):
            batch = do_rollout_single_agent(module, env, episode_length=episode_length)
            local_mixin_buffer.add(batch.as_multi_agent())
            batch_to_train_on = local_mixin_buffer.replay()
            res = learner.update(batch_to_train_on.as_multi_agent())
            module.set_state(learner.get_state()["module_state"])
            if not i % 10:
                print(res, "return", sum(batch["rewards"]))
