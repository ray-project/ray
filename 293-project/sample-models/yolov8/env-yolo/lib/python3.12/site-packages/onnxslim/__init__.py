import os
import warnings

from onnxslim.cli import slim
from onnxslim.core.pattern.registry import (
    DEFAULT_FUSION_PATTERNS,
    register_fusion_pattern,
)
from onnxslim.version import __version__

if os.path.dirname(os.path.realpath(__file__)) == os.path.join(os.path.realpath(os.getcwd()), "onnxslim"):
    message = (
        "You are importing onnxslim within its own root folder ({}). "
        "This is not expected to work and may give errors. Please exit the "
        "onnxslim project source and relaunch your python interpreter."
    )
    warnings.warn(message.format(os.getcwd()))
