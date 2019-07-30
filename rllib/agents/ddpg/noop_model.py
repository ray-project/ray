from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.models import Model
from ray.rllib.utils.annotations import override
from ray.rllib.utils import try_import_tf

tf = try_import_tf()


class NoopModel(Model):
    """Trivial model that just returns the obs flattened.

    This is the model used if use_state_preprocessor=False."""

    @override(Model)
    def _build_layers_v2(self, input_dict, num_outputs, options):
        out = tf.reshape(input_dict["obs"], [-1, num_outputs])
        return out, out
