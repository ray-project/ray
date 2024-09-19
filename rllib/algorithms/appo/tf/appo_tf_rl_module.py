from ray.rllib.algorithms.appo.appo_rl_module import APPORLModule
from ray.rllib.algorithms.ppo.tf.ppo_tf_rl_module import PPOTfRLModule


class APPOTfRLModule(PPOTfRLModule, APPORLModule):
    pass
