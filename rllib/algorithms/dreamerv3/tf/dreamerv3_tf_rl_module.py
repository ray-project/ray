"""
[1] Mastering Diverse Domains through World Models - 2023
D. Hafner, J. Pasukonis, J. Ba, T. Lillicrap
https://arxiv.org/pdf/2301.04104v1.pdf

[2] Mastering Atari with Discrete World Models - 2021
D. Hafner, T. Lillicrap, M. Norouzi, J. Ba
https://arxiv.org/pdf/2010.02193.pdf
"""
from ray.rllib.algorithms.dreamerv3.dreamerv3_rl_module import DreamerV3RLModule
from ray.rllib.core.rl_module.tf.tf_rl_module import TfRLModule
from ray.rllib.utils.framework import try_import_tf

tf1, tf, _ = try_import_tf()


class DreamerV3TfRLModule(TfRLModule, DreamerV3RLModule):
    """The tf-specific RLModule class for DreamerV3.

    Serves mainly as a thin-wrapper around the `DreamerModel` (a tf.keras.Model) class.
    """

    framework = "tf2"
