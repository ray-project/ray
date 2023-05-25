import argparse
from gymnasium.spaces import Box, Discrete
import numpy as np

from ray.rllib.examples.models.custom_model_api import (
    DuelingQModel,
    TorchDuelingQModel,
    ContActionQModel,
    TorchContActionQModel,
)
from ray.rllib.models.catalog import ModelCatalog, MODEL_DEFAULTS
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.framework import try_import_tf, try_import_torch

tf1, tf, tfv = try_import_tf()
torch, _ = try_import_torch()

parser = argparse.ArgumentParser()
parser.add_argument(
    "--framework",
    choices=["tf", "tf2", "torch"],
    default="torch",
    help="The DL framework specifier.",
)

if __name__ == "__main__":
    args = parser.parse_args()

    # Test API wrapper for dueling Q-head.

    obs_space = Box(-1.0, 1.0, (3,))
    action_space = Discrete(3)

    # Run in eager mode for value checking and debugging.
    tf1.enable_eager_execution()

    # __sphinx_doc_model_construct_1_begin__
    my_dueling_model = ModelCatalog.get_model_v2(
        obs_space=obs_space,
        action_space=action_space,
        num_outputs=action_space.n,
        model_config=MODEL_DEFAULTS,
        framework=args.framework,
        # Providing the `model_interface` arg will make the factory
        # wrap the chosen default model with our new model API class
        # (DuelingQModel). This way, both `forward` and `get_q_values`
        # are available in the returned class.
        model_interface=DuelingQModel
        if args.framework != "torch"
        else TorchDuelingQModel,
        name="dueling_q_model",
    )
    # __sphinx_doc_model_construct_1_end__

    batch_size = 10
    input_ = np.array([obs_space.sample() for _ in range(batch_size)])
    # Note that for PyTorch, you will have to provide torch tensors here.
    if args.framework == "torch":
        input_ = torch.from_numpy(input_)

    input_dict = SampleBatch(obs=input_, _is_training=False)
    out, state_outs = my_dueling_model(input_dict=input_dict)
    assert out.shape == (10, 256)
    # Pass `out` into `get_q_values`
    q_values = my_dueling_model.get_q_values(out)
    assert q_values.shape == (10, action_space.n)

    # Test API wrapper for single value Q-head from obs/action input.

    obs_space = Box(-1.0, 1.0, (3,))
    action_space = Box(-1.0, -1.0, (2,))

    # __sphinx_doc_model_construct_2_begin__
    my_cont_action_q_model = ModelCatalog.get_model_v2(
        obs_space=obs_space,
        action_space=action_space,
        num_outputs=2,
        model_config=MODEL_DEFAULTS,
        framework=args.framework,
        # Providing the `model_interface` arg will make the factory
        # wrap the chosen default model with our new model API class
        # (DuelingQModel). This way, both `forward` and `get_q_values`
        # are available in the returned class.
        model_interface=ContActionQModel
        if args.framework != "torch"
        else TorchContActionQModel,
        name="cont_action_q_model",
    )
    # __sphinx_doc_model_construct_2_end__

    batch_size = 10
    input_ = np.array([obs_space.sample() for _ in range(batch_size)])

    # Note that for PyTorch, you will have to provide torch tensors here.
    if args.framework == "torch":
        input_ = torch.from_numpy(input_)

    input_dict = SampleBatch(obs=input_, _is_training=False)
    # Note that for PyTorch, you will have to provide torch tensors here.
    out, state_outs = my_cont_action_q_model(input_dict=input_dict)
    assert out.shape == (10, 256)
    # Pass `out` and an action into `my_cont_action_q_model`
    action = np.array([action_space.sample() for _ in range(batch_size)])
    if args.framework == "torch":
        action = torch.from_numpy(action)

    q_value = my_cont_action_q_model.get_single_q_value(out, action)
    assert q_value.shape == (10, 1)
