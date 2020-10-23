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
                 shift: Union[int, str, List[int]] = 0,
                 used_for_training: bool = True):
        """Initializes a ViewRequirement object.

        Args:
            data_col (): The data column name from the SampleBatch (str key).
                If None, use the dict key under which this ViewRequirement
                resides.
            space (gym.Space): The gym Space used in case we need to pad data
                in inaccessible areas of the trajectory (t<0 or t>H).
                Default: Simple box space, e.g. rewards.
            shift (Union[int, str, List[int]]): Single shift value of list of
                shift values to use relative to the underlying `data_col`.
                Example: For a view column "prev_actions", you can set
                `data_col="actions"` and `shift=-1`.
                Example: For a view column "obs" in an Atari framestacking
                fashion, you can set `data_col="obs"` and
                `shift=[-3, -2, -1, 0]`.
                Example: For the obs input to an attention net, you can specify
                a range via a str: `shift="-100:0"`, which will pass in the
                past 100 observations plus the current one.                
            used_for_training (bool): Whether the data will be used for
                training. If False, the column will not be copied into the
                final train batch.
        """
        self.data_col = data_col
        self.space = space or gym.spaces.Box(
            float("-inf"), float("inf"), shape=())

        self.shift = shift
        # Special case: Providing a (probably larger) range of indices, e.g.
        # "-100:0" (past 100 timesteps plus current one).
        self.shift_from = self.shift_to = None
        if isinstance(self.shift, str):
            f, t = self.shift.split(":")
            self.shift_from = int(f)
            self.shift_to = int(t)

        self.used_for_training = used_for_training

        # TODO: probably need max seq-len here + flag to indicate that only first
        #  timestep per sequence is needed (for RNNs and attention nets).
        #  also will have to provide a function (in the model?) to process
        #  train_bathes (like a 0_pad-func for RNNs)
