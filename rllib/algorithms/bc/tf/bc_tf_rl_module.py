from ray.rllib.algorithms.bc.bc_rl_module import BCRLModule
from ray.rllib.core.rl_module.tf.tf_rl_module import TfRLModule


class BCTfRLModule(TfRLModule, BCRLModule):
    pass
