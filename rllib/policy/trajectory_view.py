from gym.spaces import Box, Space
import numpy as np
from typing import Dict, Optional

from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.types import TensorType

torch, _ = try_import_torch()


class ViewRequirement:
    """Single view requirement (for one column in a ModelV2 input_dict).

    Note: This is an experimental class.

    ModelV2 returns a Dict[str, ViewRequirement] upon calling
    `ModelV2.get_view_requirements()`, where the str key represents the column
    name (C) under which the view is available in the `input_dict` and
    ViewRequirement specifies the actual underlying column names (in the
    original data buffer), timesteps, and other options to build the view
    for N.

    Examples:
        >>> # The default ViewRequirement for a Model is:
        >>> req = [ModelV2].get_view_requirements(is_training=False)
        >>> print(req)
        {"obs": ViewRequirement(timesteps=0)}
    """

    def __init__(self,
                 data_col: Optional[str] = None,
                 space: Space = None,
                 timesteps: int = 0,
                 fill: str = "zeros",
                 repeat_mode: str = "all"):
        """Initializes a ViewRequirement object.

        Args:
            data_col (): The data column name from the SampleBatch (str key).
                If None, use the dict key under which this ViewRequirement
                resides.
            space (Space): The gym Space used in case we need to pad data in
                unaccessible areas of the trajectory (t<0 or t>H).
                Default: Simple box space, e.g. rewards.
            timesteps (Union[List[int], int]): List of relative (or absolute
                timesteps) to be present in the input_dict.
            fill (str): The fill mode in case t<0 or t>H.
                One of "zeros", "tile", or a TensorType to be used for filling.
            repeat_mode (str): The repeat-mode (one of "all" or "only_first").
                E.g. for training, we only want the first internal state
                timestep (the NN will calculate all others again anyways).
        """
        self.data_col = data_col
        self.space = space or Box(float("-inf"), float("inf"), shape=())
        self.timesteps = timesteps

        # Switch on absolute timestep mode. Default: False.
        # TODO: (sven)
        # "absolute_timesteps",

        self.fill = fill
        self.repeat_mode = repeat_mode

        # Provide all data as time major (default: False).
        # TODO: (sven)
        # "time_major",


def get_trajectory_view(
        model,
        trajectories,
        is_training: bool = False) -> Dict[str, TensorType]:
    """Returns an input_dict for a Model's forward pass given some data.

    Args:
        model (ModelV2): The ModelV2 object for which to generate the view
            (input_dict) from `data`.
        trajectories (List[Trajectory]): The data from which to generate
            an input_dict.
        is_training (bool): Whether the view should be generated for training
            purposes or inference (default).

    Returns:
        Dict[str, TensorType]: The input_dict to be passed into the ModelV2
            for inference/training.
    """
    # Get ModelV2's view requirements.
    view_reqs = model.get_view_requirements(is_training=is_training)

    # Construct the view dict.
    view = {}
    for view_col, view_req in view_reqs.items():
        # Create the batch of data from the different buffers in `data`.
        # TODO: (sven): Here, we actually do create a copy of the data (from a
        #   list). The only way to avoid this entirely would be to keep a
        #   single(!) np buffer per column across all currently ongoing
        #   agents + episodes (which seems very hard to realize).
        data_col = view_req.data_col or view_col
        batch = []
        for t in trajectories:
            if data_col in t.buffers:
                batch.append(t.buffers[data_col][t.cursor + view_req.timesteps])
            elif view_req.fill == "zeros":
                batch.append(np.zeros(view_req.space.shape))
            elif view_req.fill == "tile":
                batch.append(np.zeros(view_req.space.shape))
            else:
                raise NotImplementedError
        if torch and isinstance(batch[0], torch.Tensor):
            view[view_col] = torch.stack(batch)
        else:
            view[view_col] = np.array(batch)

    return view
