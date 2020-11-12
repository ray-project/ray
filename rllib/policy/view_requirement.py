import gym
import numpy as np
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
        {"obs": ViewRequirement(data_rel_pos=0)}
    """

    def __init__(self,
                 data_col: Optional[str] = None,
                 space: gym.Space = None,
                 data_rel_pos: Union[int, str, List[int]] = 0,
                 abs_pos: Optional[int] = None,
                 batch_repeat_value: int = 1,
                 used_for_training: bool = True,
                 is_input_dict: bool = False):
        """Initializes a ViewRequirement object.

        Args:
            data_col (Optional[str]): The data column name from the SampleBatch
                (str key). If None, use the dict key under which this
                ViewRequirement resides.
            space (gym.Space): The gym Space used in case we need to pad data
                in inaccessible areas of the trajectory (t<0 or t>H).
                Default: Simple box space, e.g. rewards.
            data_rel_pos (Union[int, str, List[int]]): Single shift value or
                list of relative positions to use (relative to the underlying
                `data_col`).
                Example: For a view column "prev_actions", you can set
                `data_col="actions"` and `data_rel_pos=-1`.
                Example: For a view column "obs" in an Atari framestacking
                fashion, you can set `data_col="obs"` and
                `data_rel_pos=[-3, -2, -1, 0]`.
                Example: For the obs input to an attention net, you can specify
                a range via a str: `data_rel_pos="-100:0"`, which will pass in
                the past 100 observations plus the current one.
            abs_pos (Optional[int]): An optional absolute position arg,
                used e.g. for the location of a requested inference dict within
                the trajectory. Negative values refer to counting from the end
                of a trajectory.
            used_for_training (bool): Whether the data will be used for
                training. If False, the column will not be copied into the
                final train batch.
            is_input_dict (bool): Whether the "view" of this requirement is an
                entire (inference) input dict based on the Model's
                `self.inference_view_requirements`.
        """
        self.data_col = data_col
        self.space = space or gym.spaces.Box(
            float("-inf"), float("inf"), shape=())

        self.abs_pos = abs_pos
        self.batch_repeat_value = batch_repeat_value

        self.data_rel_pos = data_rel_pos
        if isinstance(self.data_rel_pos, (list, tuple)):
            self.data_rel_pos = np.array(self.data_rel_pos)

        # Special case: Providing a (probably larger) range of indices, e.g.
        # "-100:0" (past 100 timesteps plus current one).
        self.data_rel_pos_from = self.data_rel_pos_to = None
        if isinstance(self.data_rel_pos, str):
            f, t = self.data_rel_pos.split(":")
            self.data_rel_pos_from = int(f)
            self.data_rel_pos_to = int(t)

        self.used_for_training = used_for_training

        # Whether the "view" is an entire (inference) input dict based on the
        # Model's `self.inference_view_requirements`.
        self.is_input_dict = is_input_dict
