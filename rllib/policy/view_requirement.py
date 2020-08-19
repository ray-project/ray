import gym
from typing import List, Optional, Union

from ray.rllib.utils.framework import try_import_torch

torch, _ = try_import_torch()


class ViewRequirement:
    """Single view requirement (for one column in an SampleBatch/input_dict).

    Note: This is an experimental class used only if
    `_use_trajectory_view_api` in the config is set to True.

    Policies and ModelV2s return a Dict[str, ViewRequirement] upon calling
    their `[train|inference]_view_requirements()` methods, where the str key
    represents the column name (C) under which the view is available in the
    input_dict/SampleBatch and ViewRequirement specifies the actual underlying
    column names (in the original data buffer), timestep shifts, and other
    options to build the view.

    Examples:
        >>> # The default ViewRequirement for a Model is:
        >>> req = [ModelV2].inference_view_requirements
        >>> print(req)
        {"obs": ViewRequirement(shift=0)}
    """

    def __init__(self,
                 data_col: Optional[str] = None,
                 space: gym.Space = None,
                 shift: Union[int, List[int]] = 0):
        """Initializes a ViewRequirement object.

        Args:
            data_col (): The data column name from the SampleBatch (str key).
                If None, use the dict key under which this ViewRequirement
                resides.
            space (gym.Space): The gym Space used in case we need to pad data
                in inaccessible areas of the trajectory (t<0 or t>H).
                Default: Simple box space, e.g. rewards.
            shift (Union[int, List[int]]): Single shift value of list of
                shift values to use relative to the underlying `data_col`.
                Example: For a view column "prev_actions", you can set
                `data_col="actions"` and `shift=-1`.
                Example: For a view column "obs" in an Atari framestacking
                fashion, you can set `data_col="obs"` and
                `shift=[-3, -2, -1, 0]`.
        """
        self.data_col = data_col
        self.space = space or gym.spaces.Box(
            float("-inf"), float("inf"), shape=())
        self.shift = shift
