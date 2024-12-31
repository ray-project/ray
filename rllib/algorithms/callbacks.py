# @OldAPIStack
from ray.rllib.callbacks.callbacks import Callbacks
from ray.rllib.callbacks.utils import _make_multi_callbacks


# Backward compatibility
DefaultCallbacks = Callbacks
make_multi_callbacks = _make_multi_callbacks
