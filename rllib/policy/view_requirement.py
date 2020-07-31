from gym.spaces import Box, Space
from typing import Optional

from ray.rllib.utils.framework import try_import_torch

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
                 shift: int = 0,
                 sampling: bool = True,
                 postprocessing: bool = True,
                 training: bool = True):
        """Initializes a ViewRequirement object.

        Args:
            data_col (): The data column name from the SampleBatch (str key).
                If None, use the dict key under which this ViewRequirement
                resides.
            space (Space): The gym Space used in case we need to pad data in
                unaccessible areas of the trajectory (t<0 or t>H).
                Default: Simple box space, e.g. rewards.
            shift (Union[List[int], int]): List of relative (or absolute
                timesteps) to be present in the input_dict.
            repeat_mode (str): The repeat-mode (one of "all" or "only_first").
                E.g. for training, we only want the first internal state
                timestep (the NN will calculate all others again anyways).
        """
        self.data_col = data_col
        self.space = space or Box(float("-inf"), float("inf"), shape=())
        self.shift = shift
        self.sampling = sampling
        self.postprocessing = postprocessing
        self.training = training
