import gym
import numpy as np
from typing import List, Optional, Union

from ray.rllib.utils.framework import try_import_torch

torch, _ = try_import_torch()


class ViewRequirement:
    """Single view requirement (for one column in an SampleBatch/input_dict).

    Policies and ModelV2s return a Dict[str, ViewRequirement] upon calling
    their `[train|inference]_view_requirements()` methods, where the str key
    represents the column name (C) under which the view is available in the
    input_dict/SampleBatch and ViewRequirement specifies the actual underlying
    column names (in the original data buffer), timestep shifts, and other
    options to build the view.

    Examples:
        >>> from ray.rllib.models.modelv2 import ModelV2
        >>> # The default ViewRequirement for a Model is:
        >>> req = ModelV2(...).view_requirements # doctest: +SKIP
        >>> print(req) # doctest: +SKIP
        {"obs": ViewRequirement(shift=0)}
    """

    def __init__(
        self,
        data_col: Optional[str] = None,
        space: gym.Space = None,
        shift: Union[int, str, List[int]] = 0,
        index: Optional[int] = None,
        batch_repeat_value: int = 1,
        used_for_compute_actions: bool = True,
        used_for_training: bool = True,
    ):
        """Initializes a ViewRequirement object.

        Args:
            data_col (Optional[str]): The data column name from the SampleBatch
                (str key). If None, use the dict key under which this
                ViewRequirement resides.
            space (gym.Space): The gym Space used in case we need to pad data
                in inaccessible areas of the trajectory (t<0 or t>H).
                Default: Simple box space, e.g. rewards.
            shift (Union[int, str, List[int]]): Single shift value or
                list of relative positions to use (relative to the underlying
                `data_col`).
                Example: For a view column "prev_actions", you can set
                `data_col="actions"` and `shift=-1`.
                Example: For a view column "obs" in an Atari framestacking
                fashion, you can set `data_col="obs"` and
                `shift=[-3, -2, -1, 0]`.
                Example: For the obs input to an attention net, you can specify
                a range via a str: `shift="-100:0"`, which will pass in
                the past 100 observations plus the current one.
            index (Optional[int]): An optional absolute position arg,
                used e.g. for the location of a requested inference dict within
                the trajectory. Negative values refer to counting from the end
                of a trajectory.
            used_for_compute_actions (bool): Whether the data will be used for
                creating input_dicts for `Policy.compute_actions()` calls (or
                `Policy.compute_actions_from_input_dict()`).
            used_for_training (bool): Whether the data will be used for
                training. If False, the column will not be copied into the
                final train batch.
        """
        self.data_col = data_col
        self.space = (
            space
            if space is not None
            else gym.spaces.Box(float("-inf"), float("inf"), shape=())
        )

        self.shift = shift
        if isinstance(self.shift, (list, tuple)):
            self.shift = np.array(self.shift)

        # Special case: Providing a (probably larger) range of indices, e.g.
        # "-100:0" (past 100 timesteps plus current one).
        self.shift_from = self.shift_to = None
        if isinstance(self.shift, str):
            f, t = self.shift.split(":")
            self.shift_from = int(f)
            self.shift_to = int(t)

        self.index = index
        self.batch_repeat_value = batch_repeat_value

        self.used_for_compute_actions = used_for_compute_actions
        self.used_for_training = used_for_training
