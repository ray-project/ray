from gym.spaces import Box, Discrete
import numpy as np

from ray.rllib.models.catalog import ModelCatalog, MODEL_DEFAULTS
from ray.rllib.models.tf.tf_modelv2 import TFModelV2
from ray.rllib.utils.framework import try_import_tf


tf1, tf, tfv = try_import_tf()


# __sphinx_doc_model_api_begin__

class DuelingQModel(TFModelV2):  # or: TorchModelV2
    """A simple, hard-coded dueling head model."""

    def __init__(self, obs_space, action_space, num_outputs, model_config, name):
        # Pass num_outputs=None into super constructor (so that no action/
        # logits output layer is built).
        # Alternatively, you can pass in num_outputs=[last layer size of
        # config[model][fcnet_hiddens]] AND set no_last_linear=True, but
        # this seems more tedious as you will have to explain users of this
        # class that num_outputs is NOT the size of your Q-output layer.
        super(DuelingQModel, self).__init__(
            obs_space, action_space, None, model_config, name)
        # Now: self.num_outputs contains the last layer's size, which
        # we can use to construct the dueling head (see torch: SlimFC
        # below).

        # Construct advantage head ...
        self.A = tf.keras.layers.Dense(num_outputs)
        # torch:
        # self.A = SlimFC(
        #     in_size=self.num_outputs, out_size=num_outputs)

        # ... and value head.
        self.V = tf.keras.layers.Dense(1)
        # torch:
        # self.V = SlimFC(in_size=self.num_outputs, out_size=1)

    def get_q_values(self, underlying_output):
        # Calculate q-values following dueling logic:
        v = self.V(underlying_output)  # value
        a = self.A(underlying_output)  # advantages (per action)
        advantages_mean = tf.reduce_mean(a, 1)
        advantages_centered = a - tf.expand_dims(advantages_mean, 1)
        return v + advantages_centered  # q-values

# __sphinx_doc_model_api_end__


if __name__ == "__main__":
    obs_space = Box(-1.0, 1.0, (3, ))
    action_space = Discrete(3)

    # Run in eager mode for value checking and debugging.
    tf1.enable_eager_execution()

    # __sphinx_doc_model_construct_begin__
    my_dueling_model = ModelCatalog.get_model_v2(
        obs_space=obs_space,
        action_space=action_space,
        num_outputs=action_space.n,
        model_config=MODEL_DEFAULTS,
        framework="tf",  # or: "torch"
        # Providing the `model_interface` arg will make the factory
        # wrap the chosen default model with our new model API class
        # (DuelingQModel). This way, both `forward` and `get_q_values`
        # are available in the returned class.
        model_interface=DuelingQModel,
        name="dueling_q_model",
    )
    # __sphinx_doc_model_construct_end__

    batch_size = 10
    input_ = np.array([obs_space.sample() for _ in range(batch_size)])
    input_dict = {
        "obs": input_,
        "is_training": False,
    }
    # Note that for PyTorch, you will have to provide torch tensors here.
    out, state_outs = my_dueling_model(input_dict=input_dict)
    assert out.shape == (10, 256)
    # Pass `out` into `get_q_values`
    q_values = my_dueling_model.get_q_values(out)
    assert q_values.shape == (10, action_space.n)
