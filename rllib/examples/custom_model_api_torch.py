from gym.spaces import Box, Discrete
import numpy as np

from ray.rllib.models.catalog import ModelCatalog, MODEL_DEFAULTS
from ray.rllib.utils.framework import try_import_torch


torch, _ = try_import_torch()


if __name__ == "__main__":
    obs_space = Box(-1.0, 1.0, (3, ))
    action_space = Box(-1.0, -1.0, (2, ))

    # __sphinx_doc_model_construct_begin__
    my_cont_action_q_model = ModelCatalog.get_model_v2(
        obs_space=obs_space,
        action_space=action_space,
        num_outputs=2,
        model_config=MODEL_DEFAULTS,
        framework="torch",  # or: "tf"
        # Providing the `model_interface` arg will make the factory
        # wrap the chosen default model with our new model API class
        # (DuelingQModel). This way, both `forward` and `get_q_values`
        # are available in the returned class.
        model_interface=ContActionQModel,
        name="cont_action_q_model",
    )
    # __sphinx_doc_model_construct_end__

    batch_size = 10
    input_ = torch.from_numpy(
        np.array([obs_space.sample() for _ in range(batch_size)]))
    input_dict = {
        "obs": input_,
        "is_training": False,
    }
    # Note that for PyTorch, you will have to provide torch tensors here.
    out, state_outs = my_cont_action_q_model(input_dict=input_dict)
    assert out.shape == (10, 256)
    # Pass `out` and an action into `my_cont_action_q_model`
    action = torch.from_numpy(
        np.array([action_space.sample() for _ in range(batch_size)]))
    q_value = my_cont_action_q_model.get_single_q_value(out, action)
    assert q_value.shape == (10, 1)
