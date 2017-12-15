from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


def get_policy_cls(config):
    if config["use_lstm"]:
        from ray.rllib.a3c.shared_model_lstm import SharedModelLSTM
        policy_cls = SharedModelLSTM
    elif config["use_pytorch"]:
        from ray.rllib.a3c.shared_torch_policy import SharedTorchPolicy
        policy_cls = SharedTorchPolicy
    else:
        from ray.rllib.a3c.shared_model import SharedModel
        policy_cls = SharedModel
    return policy_cls
