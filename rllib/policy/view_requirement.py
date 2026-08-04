import dataclasses
from typing import Dict, List, Optional, Union

import gymnasium as gym
import numpy as np

from ray.rllib.utils.annotations import OldAPIStack
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.serialization import (
    gym_space_from_dict,
    gym_space_to_dict,
)

torch, _ = try_import_torch()


@OldAPIStack
@dataclasses.dataclass
class ViewRequirement:
    """Single view requirement (for one column in an SampleBatch/input_dict).

    Policies and ModelV2s return a Dict[str, ViewRequirement] upon calling
    their `[train|inference]_view_requirements()` methods, where the str key
    represents the column name (C) under which the view is available in the
    input_dict/SampleBatch and ViewRequirement specifies the actual underlying
    column names (in the original data buffer), timestep shifts, and other
    options to build the view.

    .. testcode::
        :skipif: True

        from ray.rllib.models.modelv2 import ModelV2
        # The default ViewRequirement for a Model is:
        req = ModelV2(...).view_requirements
        print(req)

    .. testoutput::

        {"obs": ViewRequirement(shift=0)}

    Args:
        data_col: The data column name from the SampleBatch
            (str key). If None, use the dict key under which this
            ViewRequirement resides.
        space: The gym Space used in case we need to pad data
            in inaccessible areas of the trajectory (t<0 or t>H).
            Default: Simple box space, e.g. rewards.
        shift: Single shift value or
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
        index: An optional absolute position arg,
            used e.g. for the location of a requested inference dict within
            the trajectory. Negative values refer to counting from the end
            of a trajectory. (#TODO: Is this still used?)
        batch_repeat_value: determines how many time steps we should skip
            before we repeat the view indexing for the next timestep. For RNNs this
            number is usually the sequence length that we will rollout over.
            Example:
                view_col = "state_in_0", data_col = "state_out_0"
                batch_repeat_value = 5, shift = -1
                buffer["state_out_0"] = [-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
                output["state_in_0"] = [-1, 4, 9]
            Explanation: For t=0, we output buffer["state_out_0"][-1]. We then skip 5
            time steps and repeat the view. for t=5, we output buffer["state_out_0"][4]
            . Continuing on this pattern, for t=10, we output buffer["state_out_0"][9].
        used_for_compute_actions: Whether the data will be used for
            creating input_dicts for `Policy.compute_actions()` calls (or
            `Policy.compute_actions_from_input_dict()`).
        used_for_training: Whether the data will be used for
            training. If False, the column will not be copied into the
            final train batch.
    """

    data_col: Optional[str] = None
    space: gym.Space = None
    shift: Union[int, str, List[int]] = 0
    index: Optional[int] = None
    batch_repeat_value: int = 1
    used_for_compute_actions: bool = True
    used_for_training: bool = True
    shift_arr: Optional[np.ndarray] = dataclasses.field(init=False)

    def __post_init__(self):
        """Initializes a ViewRequirement object.

        shift_arr is infered from the shift value.

        For example:
            - if shift is -1, then shift_arr is np.array([-1]).
            - if shift is [-1, -2], then shift_arr is np.array([-2, -1]).
            - if shift is "-2:2", then shift_arr is np.array([-2, -1, 0, 1, 2]).
        """

        if self.space is None:
            self.space = gym.spaces.Box(float("-inf"), float("inf"), shape=())

        # TODO: ideally we won't need shift_from and shift_to, and shift_step.
        # all of them should be captured within shift_arr.
        # Special case: Providing a (probably larger) range of indices, e.g.
        # "-100:0" (past 100 timesteps plus current one).
        self.shift_from = self.shift_to = self.shift_step = None
        if isinstance(self.shift, str):
            split = self.shift.split(":")
            assert len(split) in [2, 3], f"Invalid shift str format: {self.shift}"
            if len(split) == 2:
                f, t = split
                self.shift_step = 1
            else:
                f, t, s = split
                self.shift_step = int(s)

            self.shift_from = int(f)
            self.shift_to = int(t)

        shift = self.shift
        self.shfit_arr = None
        if self.shift_from:
            self.shift_arr = np.arange(
                self.shift_from, self.shift_to + 1, self.shift_step
            )
        else:
            if isinstance(shift, int):
                self.shift_arr = np.array([shift])
            elif isinstance(shift, list):
                self.shift_arr = np.array(shift)
            else:
                ValueError(f'unrecognized shift type: "{shift}"')

    def to_dict(self) -> Dict:
        """Return a dict for this ViewRequirement that can be JSON serialized."""
        return {
            "data_col": self.data_col,
            "space": gym_space_to_dict(self.space),
            "shift": self.shift,
            "index": self.index,
            "batch_repeat_value": self.batch_repeat_value,
            "used_for_training": self.used_for_training,
            "used_for_compute_actions": self.used_for_compute_actions,
        }

    @classmethod
    def from_dict(cls, d: Dict):
        """Construct a ViewRequirement instance from JSON deserialized dict."""
        d["space"] = gym_space_from_dict(d["space"])
        return cls(**d)
