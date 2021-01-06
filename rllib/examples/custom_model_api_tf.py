from gym.spaces import Box, Discrete
import numpy as np

from ray.rllib.models.catalog import ModelCatalog, MODEL_DEFAULTS


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
