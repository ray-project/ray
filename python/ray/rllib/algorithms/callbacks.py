# @OldAPIStack
from ray.rllib.callbacks.callbacks import RLlibCallback
from ray.rllib.callbacks.utils import _make_multi_callbacks

# Backward compatibility
DefaultCallbacks = RLlibCallback
make_multi_callbacks = _make_multi_callbacks
