from ray.rllib.algorithms.appo.appo_rl_module import APPORLModule
from ray.rllib.algorithms.ppo.tf.ppo_tf_rl_module import PPOTfRLModule
from ray.rllib.utils.framework import try_import_tf

_, tf, _ = try_import_tf()


class APPOTfRLModule(PPOTfRLModule, APPORLModule):
    pass
