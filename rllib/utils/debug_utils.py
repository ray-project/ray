"""Module for validating and checking various RLlib components."""
import logging
from typing import Dict

from ray.rllib.utils.framework import try_import_tf, try_import_torch

logger = logging.getLogger(__name__)


def validate_config(config: Dict) -> None:
    """Validates the config dictionary.

    Args:
        config: The config dictionary to be validated.

    """
    _tf1, _tf, _tfv = None, None, None
    _torch = None
    framework = config["framework"]
    tf_valid_frameworks = {"tf", "tf2", "tfe"}
    if framework not in tf_valid_frameworks and framework != "torch":
        return
    elif framework in tf_valid_frameworks:
        _tf1, _tf, _tfv = try_import_tf()
    else:
        _torch, _ = try_import_torch()

    def check_if_correct_nn_framework_installed():
        """Check if tf/torch experiment is running and tf/torch is installed.
        """
        if framework in tf_valid_frameworks:
            if not (_tf1 or _tf):
                raise ImportError(
                    ("TensorFlow, was the specified as the 'framework' inside "
                     "of your config dictionary, however, there was no "
                     "installation found. You can install tensorflow via "
                     "pip: pip install tensorflow"))
        elif framework == "torch":
            if not _torch:
                raise ImportError(
                    ("torch, was the specified as the 'framework' inside "
                     "of your config dictionary, however, there was no "
                     "installation found. You can install torch via "
                     "pip: pip install torch"))

    def resolve_tf_settings():
        """Check and resolve DL framework settings.
        Tf-eager (tf2|tfe), possibly with tracing set to True. Recommend
        setting tracing to True for speedups.
        """

        if _tf1:
            if framework == "tf2" and _tfv < 2:
                raise ValueError(
                    "You configured `framework`=tf2, but your installed pip "
                    "tf-version is < 2.0! Make sure your TensorFlow version "
                    "is >= 2.x.")
            if not _tf1.executing_eagerly():
                _tf1.enable_eager_execution()
            logger.info(
                f"Executing eagerly (framework='{framework}'),"
                f" with eager_tracing={config['eager_tracing']}. For "
                "production workloads, make sure to set `eager_tracing=True` "
                "in order to match the speed of tf-static-graph "
                "(framework='tf'). For debugging purposes, "
                "`eager_tracing=False` is the best choice.")
        # Tf-static-graph (framework=tf): Recommend upgrading to tf2 and
        # enabling eager tracing for similar speed.
        elif _tf1 and framework == "tf":
            logger.info(
                "Your framework setting is 'tf', meaning you are using static"
                "-graph mode. Set framework='tf2' to enable eager execution "
                "with tf2.x. You may also want to then set "
                "`eager_tracing=True` in order to reach similar execution "
                "speed as with static-graph mode.")

    check_if_correct_nn_framework_installed()
    resolve_tf_settings()
