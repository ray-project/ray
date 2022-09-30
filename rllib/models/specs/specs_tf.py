
from ray.rllib.utils.framework import try_import_tf

from .specs_base import TensorSpecs

_, tf, tfv = try_import_tf()

class TFSpecs(TensorSpecs):
    # TODO
    pass