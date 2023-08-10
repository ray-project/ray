import collections
from functools import partial
import itertools
import sys
from numbers import Number
from typing import Dict, Iterator, Set, Union
from typing import List, Optional

import numpy as np
import tree  # pip install dm_tree

from ray.rllib.utils.annotations import DeveloperAPI, ExperimentalAPI, PublicAPI
from ray.rllib.utils.compression import pack, unpack, is_compressed
from ray.rllib.utils.deprecation import Deprecated, deprecation_warning
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.torch_utils import convert_to_torch_tensor
from ray.rllib.utils.typing import (
    PolicyID,
    TensorType,
    SampleBatchType,
    ViewRequirementsDict,
)
from ray.util import log_once

tf1, tf, tfv = try_import_tf()
torch, _ = try_import_torch()

# Default policy id for single agent environments
DEFAULT_POLICY_ID = "default_policy"


@DeveloperAPI
def attempt_count_timesteps(tensor_dict: dict):
    """Attempt to count timesteps based on dimensions of individual elements.

    Returns the first successfully counted number of timesteps.
    We do not attempt to count on INFOS or any state_in_* and state_out_* keys. The
    number of timesteps we count in cases where we are unable to count is zero.

    Args:
        tensor_dict: A SampleBatch or another dict.

    Returns:
        count: The inferred number of timesteps >= 0.
    """
    # Try to infer the "length" of the SampleBatch by finding the first
    # value that is actually a ndarray/tensor.
    # Skip manual counting routine if we can directly infer count from sequence lengths
    if (
        tensor_dict.get(SampleBatch.SEQ_LENS) is not None
        and not (tf and tf.is_tensor(tensor_dict[SampleBatch.SEQ_LENS]))
        and len(tensor_dict[SampleBatch.SEQ_LENS]) > 0
    ):
        if torch and torch.is_tensor(tensor_dict[SampleBatch.SEQ_LENS]):
            return tensor_dict[SampleBatch.SEQ_LENS].sum().item()
        else:
            return sum(tensor_dict[SampleBatch.SEQ_LENS])

    for k, v in tensor_dict.items():
        if k == SampleBatch.SEQ_LENS:
            continue

        assert isinstance(k, str), tensor_dict

        if (
            k == SampleBatch.INFOS
            or k.startswith("state_in_")
            or k.startswith("state_out_")
        ):
            # Don't attempt to count on infos since we make no assumptions
            # about its content
            # Don't attempt to count on state since nesting can potentially mess
            # things up
            continue

        # If this is a nested dict (for example a nested observation),
        # try to flatten it, assert that all elements have the same length (batch
        # dimension)
        v_list = tree.flatten(v) if isinstance(v, (dict, tuple)) else [v]
        # TODO: Drop support for lists and Numbers as values.
        # If v_list contains lists or Numbers, convert them to arrays, too.
        v_list = [
            np.array(_v) if isinstance(_v, (Number, list)) else _v for _v in v_list
        ]
        try:
            # Add one of the elements' length, since they are all the same
            _len = len(v_list[0])
            if _len:
                return _len
        except Exception:
            pass

    # Return zero if we are unable to count
    return 0


@PublicAPI
class SampleBatch(dict):
    """Wrapper around a dictionary with string keys and array-like values.

    For example, {"obs": [1, 2, 3], "reward": [0, -1, 1]} is a batch of three
    samples, each with an "obs" and "reward" attribute.
    """

    # On rows in SampleBatch:
    # Each comment signifies how values relate to each other within a given row.
    # A row generally signifies one timestep. Most importantly, at t=0, SampleBatch.OBS
    # will usually be the reset-observation, while SampleBatch.ACTIONS will be the
    # action based on the reset-observation and so on. This scheme is derived from
    # RLlib's sampling logic.

    # Outputs from interacting with the environment:

    # Observation that we compute SampleBatch.ACTIONS from.
    OBS = "obs"
    # Observation returned after stepping with SampleBatch.ACTIONS.
    NEXT_OBS = "new_obs"
    # Action based on SampleBatch.OBS.
    ACTIONS = "actions"
    # Reward returned after stepping with SampleBatch.ACTIONS.
    REWARDS = "rewards"
    # Action chosen before SampleBatch.ACTIONS.
    PREV_ACTIONS = "prev_actions"
    # Reward received before SampleBatch.REWARDS.
    PREV_REWARDS = "prev_rewards"
    # Is the episode finished after stepping via SampleBatch.ACTIONS?
    TERMINATEDS = "terminateds"
    # Is the episode truncated (e.g. time limit) after stepping via SampleBatch.ACTIONS?
    TRUNCATEDS = "truncateds"
    # Infos returned after stepping with SampleBatch.ACTIONS
    INFOS = "infos"

    # Additional keys filled by RLlib to manage the data above:

    SEQ_LENS = "seq_lens"  # Groups rows into sequences by defining their length.
    T = "t"  # Timestep counter
    EPS_ID = "eps_id"  # Uniquely identifies an episode
    ENV_ID = "env_id"  # An env ID (e.g. the index for a vectorized sub-env).
    AGENT_INDEX = "agent_index"  # Uniquely identifies an agent within an episode.

    # Uniquely identifies a sample batch. This is important to distinguish RNN
    # sequences from the same episode when multiple sample batches are
    # concatenated (fusing sequences across batches can be unsafe).
    UNROLL_ID = "unroll_id"

    # Algorithm-specific keys:

    # Extra action fetches keys.
    ACTION_DIST_INPUTS = "action_dist_inputs"
    ACTION_PROB = "action_prob"
    ACTION_LOGP = "action_logp"
    ACTION_DIST = "action_dist"

    # Value function predictions emitted by the behaviour policy.
    VF_PREDS = "vf_preds"
    # Values one ts beyond the last ts taken. These are usually calculated via the value
    # function network using the final observation (and in case of an RNN: the last
    # returned internal state).
    VALUES_BOOTSTRAPPED = "values_bootstrapped"

    # RE 3
    # This is only computed and used when RE3 exploration strategy is enabled.
    OBS_EMBEDS = "obs_embeds"

    # Decision Transformer
    RETURNS_TO_GO = "returns_to_go"
    ATTENTION_MASKS = "attention_masks"

    # Deprecated keys:

    # Do not set this key directly. Instead, the values under this key are
    # auto-computed via the values of the TERMINATEDS and TRUNCATEDS keys.
    DONES = "dones"
    # Use SampleBatch.OBS instead.
    CUR_OBS = "obs"

    @PublicAPI
    def __init__(self, *args, **kwargs):
        """Constructs a sample batch (same params as dict constructor).

        Note: All args and those kwargs not listed below will be passed
        as-is to the parent dict constructor.

        Args:
            _time_major: Whether data in this sample batch
                is time-major. This is False by default and only relevant
                if the data contains sequences.
            _max_seq_len: The max sequence chunk length
                if the data contains sequences.
            _zero_padded: Whether the data in this batch
                contains sequences AND these sequences are right-zero-padded
                according to the `_max_seq_len` setting.
            _is_training: Whether this batch is used for
                training. If False, batch may be used for e.g. action
                computations (inference).
        """

        if SampleBatch.DONES in kwargs:
            raise KeyError(
                "SampleBatch cannot be constructed anymore with a `DONES` key! "
                "Instead, set the new TERMINATEDS and TRUNCATEDS keys. The values under"
                " DONES will then be automatically computed using terminated|truncated."
            )

        # Possible seq_lens (TxB or BxT) setup.
        self.time_major = kwargs.pop("_time_major", None)
        # Maximum seq len value.
        self.max_seq_len = kwargs.pop("_max_seq_len", None)
        # Is alredy right-zero-padded?
        self.zero_padded = kwargs.pop("_zero_padded", False)
        # Whether this batch is used for training (vs inference).
        self._is_training = kwargs.pop("_is_training", None)
        # Weighted average number of grad updates that have been performed on the
        # policy/ies that were used to collect this batch.
        # E.g.: Two rollout workers collect samples of 50ts each
        # (rollout_fragment_length=50). One of them has a policy that has undergone
        # 2 updates thus far, the other worker uses a policy that has undergone 3
        # updates thus far. The train batch size is 100, so we concatenate these 2
        # batches to a new one that's 100ts long. This new 100ts batch will have its
        # `num_gradient_updates` property set to 2.5 as it's the weighted average
        # (both original batches contribute 50%).
        self.num_grad_updates: Optional[float] = kwargs.pop("_num_grad_updates", None)

        # Call super constructor. This will make the actual data accessible
        # by column name (str) via e.g. self["some-col"].
        dict.__init__(self, *args, **kwargs)

        # Indicates whether, for this batch, sequence lengths should be slices by
        # their index in the batch or by their index as a sequence.
        # This is useful if a batch contains tensors of shape (B, T, ...), where each
        # index of B indicates one sequence. In this case, when slicing the batch,
        # we want one sequence to be slices out per index in B (
        # `_slice_seq_lens_by_batch_index=True`. However, if the padded batch
        # contains tensors of shape (B*T, ...), where each index of B*T indicates
        # one timestep, we want one sequence to be sliced per T steps in B*T (
        # `self._slice_seq_lens_in_B=False`).
        # ._slice_seq_lens_in_B = True is only meant to be used for batches that we
        # feed into Learner._update(), all other places in RLlib are not expected to
        # need this.
        self._slice_seq_lens_in_B = False

        self.accessed_keys = set()
        self.added_keys = set()
        self.deleted_keys = set()
        self.intercepted_values = {}
        self.get_interceptor = None

        # Clear out None seq-lens.
        seq_lens_ = self.get(SampleBatch.SEQ_LENS)
        if seq_lens_ is None or (isinstance(seq_lens_, list) and len(seq_lens_) == 0):
            self.pop(SampleBatch.SEQ_LENS, None)
        # Numpyfy seq_lens if list.
        elif isinstance(seq_lens_, list):
            self[SampleBatch.SEQ_LENS] = seq_lens_ = np.array(seq_lens_, dtype=np.int32)
        elif (torch and torch.is_tensor(seq_lens_)) or (tf and tf.is_tensor(seq_lens_)):
            self[SampleBatch.SEQ_LENS] = seq_lens_

        if (
            self.max_seq_len is None
            and seq_lens_ is not None
            and not (tf and tf.is_tensor(seq_lens_))
            and len(seq_lens_) > 0
        ):
            if torch and torch.is_tensor(seq_lens_):
                self.max_seq_len = seq_lens_.max().item()
            else:
                self.max_seq_len = max(seq_lens_)

        if self._is_training is None:
            self._is_training = self.pop("is_training", False)

        for k, v in self.items():
            # TODO: Drop support for lists and Numbers as values.
            # Convert lists of int|float into numpy arrays make sure all data
            # has same length.
            if isinstance(v, (Number, list)) and not k == SampleBatch.INFOS:
                self[k] = np.array(v)

        self.count = attempt_count_timesteps(self)

        # A convenience map for slicing this batch into sub-batches along
        # the time axis. This helps reduce repeated iterations through the
        # batch's seq_lens array to find good slicing points. Built lazily
        # when needed.
        self._slice_map = []

    @PublicAPI
    def __len__(self) -> int:
        """Returns the amount of samples in the sample batch."""
        return self.count

    @PublicAPI
    def agent_steps(self) -> int:
        """Returns the same as len(self) (number of steps in this batch).

        To make this compatible with `MultiAgentBatch.agent_steps()`.
        """
        return len(self)

    @PublicAPI
    def env_steps(self) -> int:
        """Returns the same as len(self) (number of steps in this batch).

        To make this compatible with `MultiAgentBatch.env_steps()`.
        """
        return len(self)

    @DeveloperAPI
    def enable_slicing_by_batch_id(self):
        self._slice_seq_lens_in_B = True

    @DeveloperAPI
    def disable_slicing_by_batch_id(self):
        self._slice_seq_lens_in_B = False

    @ExperimentalAPI
    def is_terminated_or_truncated(self) -> bool:
        """Returns True if `self` is either terminated or truncated at idx -1."""
        return self[SampleBatch.TERMINATEDS][-1] or (
            SampleBatch.TRUNCATEDS in self and self[SampleBatch.TRUNCATEDS][-1]
        )

    @ExperimentalAPI
    def is_single_trajectory(self) -> bool:
        """Returns True if this SampleBatch only contains one trajectory.

        This is determined by checking all timesteps (except for the last) for being
        not terminated AND (if applicable) not truncated.
        """
        return not any(self[SampleBatch.TERMINATEDS][:-1]) and (
            SampleBatch.TRUNCATEDS not in self
            or not any(self[SampleBatch.TRUNCATEDS][:-1])
        )

    @staticmethod
    @PublicAPI
    @Deprecated(new="concat_samples() from rllib.policy.sample_batch", error=False)
    def concat_samples(
        samples: Union[List["SampleBatch"], List["MultiAgentBatch"]],
    ) -> Union["SampleBatch", "MultiAgentBatch"]:
        return concat_samples(samples)

    @PublicAPI
    def concat(self, other: "SampleBatch") -> "SampleBatch":
        """Concatenates `other` to this one and returns a new SampleBatch.

        Args:
            other: The other SampleBatch object to concat to this one.

        Returns:
            The new SampleBatch, resulting from concating `other` to `self`.

        Examples:
            >>> import numpy as np
            >>> from ray.rllib.policy.sample_batch import SampleBatch
            >>> b1 = SampleBatch({"a": np.array([1, 2])}) # doctest: +SKIP
            >>> b2 = SampleBatch({"a": np.array([3, 4, 5])}) # doctest: +SKIP
            >>> print(b1.concat(b2)) # doctest: +SKIP
            {"a": np.array([1, 2, 3, 4, 5])}
        """
        return concat_samples([self, other])

    @PublicAPI
    def copy(self, shallow: bool = False) -> "SampleBatch":
        """Creates a deep or shallow copy of this SampleBatch and returns it.

        Args:
            shallow: Whether the copying should be done shallowly.

        Returns:
            A deep or shallow copy of this SampleBatch object.
        """
        copy_ = {k: v for k, v in self.items()}
        data = tree.map_structure(
            lambda v: (
                np.array(v, copy=not shallow) if isinstance(v, np.ndarray) else v
            ),
            copy_,
        )
        copy_ = SampleBatch(
            data,
            _time_major=self.time_major,
            _zero_padded=self.zero_padded,
            _max_seq_len=self.max_seq_len,
            _num_grad_updates=self.num_grad_updates,
        )
        copy_.set_get_interceptor(self.get_interceptor)
        copy_.added_keys = self.added_keys
        copy_.deleted_keys = self.deleted_keys
        copy_.accessed_keys = self.accessed_keys
        return copy_

    @PublicAPI
    def rows(self) -> Iterator[Dict[str, TensorType]]:
        """Returns an iterator over data rows, i.e. dicts with column values.

        Note that if `seq_lens` is set in self, we set it to 1 in the rows.

        Yields:
            The column values of the row in this iteration.

        Examples:
            >>> from ray.rllib.policy.sample_batch import SampleBatch
            >>> batch = SampleBatch({ # doctest: +SKIP
            ...    "a": [1, 2, 3],
            ...    "b": [4, 5, 6],
            ...    "seq_lens": [1, 2]
            ... })
            >>> for row in batch.rows(): # doctest: +SKIP
            ...    print(row) # doctest: +SKIP
            {"a": 1, "b": 4, "seq_lens": 1}
            {"a": 2, "b": 5, "seq_lens": 1}
            {"a": 3, "b": 6, "seq_lens": 1}
        """

        seq_lens = None if self.get(SampleBatch.SEQ_LENS, 1) is None else 1

        self_as_dict = {k: v for k, v in self.items()}

        for i in range(self.count):
            yield tree.map_structure_with_path(
                lambda p, v: v[i] if p[0] != self.SEQ_LENS else seq_lens,
                self_as_dict,
            )

    @PublicAPI
    def columns(self, keys: List[str]) -> List[any]:
        """Returns a list of the batch-data in the specified columns.

        Args:
            keys: List of column names fo which to return the data.

        Returns:
            The list of data items ordered by the order of column
            names in `keys`.

        Examples:
            >>> from ray.rllib.policy.sample_batch import SampleBatch
            >>> batch = SampleBatch({"a": [1], "b": [2], "c": [3]}) # doctest: +SKIP
            >>> print(batch.columns(["a", "b"])) # doctest: +SKIP
            [[1], [2]]
        """

        # TODO: (sven) Make this work for nested data as well.
        out = []
        for k in keys:
            out.append(self[k])
        return out

    @PublicAPI
    def shuffle(self) -> "SampleBatch":
        """Shuffles the rows of this batch in-place.

        Returns:
            This very (now shuffled) SampleBatch.

        Raises:
            ValueError: If self[SampleBatch.SEQ_LENS] is defined.

        Examples:
            >>> from ray.rllib.policy.sample_batch import SampleBatch
            >>> batch = SampleBatch({"a": [1, 2, 3, 4]})  # doctest: +SKIP
            >>> print(batch.shuffle()) # doctest: +SKIP
            {"a": [4, 1, 3, 2]}
        """

        # Shuffling the data when we have `seq_lens` defined is probably
        # a bad idea!
        if self.get(SampleBatch.SEQ_LENS) is not None:
            raise ValueError(
                "SampleBatch.shuffle not possible when your data has "
                "`seq_lens` defined!"
            )

        # Get a permutation over the single items once and use the same
        # permutation for all the data (otherwise, data would become
        # meaningless).
        permutation = np.random.permutation(self.count)

        self_as_dict = {k: v for k, v in self.items()}
        shuffled = tree.map_structure(lambda v: v[permutation], self_as_dict)
        self.update(shuffled)
        # Flush cache such that intercepted values are recalculated after the
        # shuffling.
        self.intercepted_values = {}
        return self

    @PublicAPI
    def split_by_episode(self, key: Optional[str] = None) -> List["SampleBatch"]:
        """Splits by `eps_id` column and returns list of new batches.
        If `eps_id` is not present, splits by `dones` instead.

        Args:
            key: If specified, overwrite default and use key to split.

        Returns:
            List of batches, one per distinct episode.

        Raises:
            KeyError: If the `eps_id` AND `dones` columns are not present.

        Examples:
            >>> from ray.rllib.policy.sample_batch import SampleBatch
            >>> # "eps_id" is present
            >>> batch = SampleBatch( # doctest: +SKIP
            ...     {"a": [1, 2, 3], "eps_id": [0, 0, 1]})
            >>> print(batch.split_by_episode()) # doctest: +SKIP
            [{"a": [1, 2], "eps_id": [0, 0]}, {"a": [3], "eps_id": [1]}]
            >>>
            >>> # "eps_id" not present, split by "dones" instead
            >>> batch = SampleBatch( # doctest: +SKIP
            ...     {"a": [1, 2, 3, 4, 5], "dones": [0, 0, 1, 0, 1]})
            >>> print(batch.split_by_episode()) # doctest: +SKIP
            [{"a": [1, 2, 3], "dones": [0, 0, 1]}, {"a": [4, 5], "dones": [0, 1]}]
            >>>
            >>> # The last episode is appended even if it does not end with done
            >>> batch = SampleBatch( # doctest: +SKIP
            ...     {"a": [1, 2, 3, 4, 5], "dones": [0, 0, 1, 0, 0]})
            >>> print(batch.split_by_episode()) # doctest: +SKIP
            [{"a": [1, 2, 3], "dones": [0, 0, 1]}, {"a": [4, 5], "dones": [0, 0]}]
            >>> batch = SampleBatch( # doctest: +SKIP
            ...     {"a": [1, 2, 3, 4, 5], "dones": [0, 0, 0, 0, 0]})
            >>> print(batch.split_by_episode()) # doctest: +SKIP
            [{"a": [1, 2, 3, 4, 5], "dones": [0, 0, 0, 0, 0]}]
        """

        assert key is None or key in [SampleBatch.EPS_ID, SampleBatch.DONES], (
            f"`SampleBatch.split_by_episode(key={key})` invalid! "
            f"Must be [None|'dones'|'eps_id']."
        )

        def slice_by_eps_id():
            slices = []
            # Produce a new slice whenever we find a new episode ID.
            cur_eps_id = self[SampleBatch.EPS_ID][0]
            offset = 0
            for i in range(self.count):
                next_eps_id = self[SampleBatch.EPS_ID][i]
                if next_eps_id != cur_eps_id:
                    slices.append(self[offset:i])
                    offset = i
                    cur_eps_id = next_eps_id
            # Add final slice.
            slices.append(self[offset : self.count])
            return slices

        def slice_by_terminateds_or_truncateds():
            slices = []
            offset = 0
            for i in range(self.count):
                if self[SampleBatch.TERMINATEDS][i] or (
                    SampleBatch.TRUNCATEDS in self and self[SampleBatch.TRUNCATEDS][i]
                ):
                    # Since self[i] is the last timestep of the episode,
                    # append it to the batch, then set offset to the start
                    # of the next batch
                    slices.append(self[offset : i + 1])
                    offset = i + 1
            # Add final slice.
            if offset != self.count:
                slices.append(self[offset:])
            return slices

        key_to_method = {
            SampleBatch.EPS_ID: slice_by_eps_id,
            SampleBatch.DONES: slice_by_terminateds_or_truncateds,
        }

        # If key not specified, default to this order.
        key_resolve_order = [SampleBatch.EPS_ID, SampleBatch.DONES]

        slices = None
        if key is not None:
            # If key specified, directly use it.
            if key == SampleBatch.EPS_ID and key not in self:
                raise KeyError(f"{self} does not have key `{key}`!")
            slices = key_to_method[key]()
        else:
            # If key not specified, go in order.
            for key in key_resolve_order:
                if key == SampleBatch.DONES or key in self:
                    slices = key_to_method[key]()
                    break
            if slices is None:
                raise KeyError(f"{self} does not have keys {key_resolve_order}!")

        assert (
            sum(s.count for s in slices) == self.count
        ), f"Calling split_by_episode on {self} returns {slices}"
        f"which should in total have {self.count} timesteps!"
        return slices

    def slice(
        self, start: int, end: int, state_start=None, state_end=None
    ) -> "SampleBatch":
        """Returns a slice of the row data of this batch (w/o copying).

        Args:
            start: Starting index. If < 0, will left-zero-pad.
            end: Ending index.

        Returns:
            A new SampleBatch, which has a slice of this batch's data.
        """
        if (
            self.get(SampleBatch.SEQ_LENS) is not None
            and len(self[SampleBatch.SEQ_LENS]) > 0
        ):
            if start < 0:
                data = {
                    k: np.concatenate(
                        [
                            np.zeros(shape=(-start,) + v.shape[1:], dtype=v.dtype),
                            v[0:end],
                        ]
                    )
                    for k, v in self.items()
                    if k != SampleBatch.SEQ_LENS and not k.startswith("state_in_")
                }
            else:
                data = {
                    k: tree.map_structure(lambda s: s[start:end], v)
                    for k, v in self.items()
                    if k != SampleBatch.SEQ_LENS and not k.startswith("state_in_")
                }
            if state_start is not None:
                assert state_end is not None
                state_idx = 0
                state_key = "state_in_{}".format(state_idx)
                while state_key in self:
                    data[state_key] = self[state_key][state_start:state_end]
                    state_idx += 1
                    state_key = "state_in_{}".format(state_idx)
                seq_lens = list(self[SampleBatch.SEQ_LENS][state_start:state_end])
                # Adjust seq_lens if necessary.
                data_len = len(data[next(iter(data))])
                if sum(seq_lens) != data_len:
                    assert sum(seq_lens) > data_len
                    seq_lens[-1] = data_len - sum(seq_lens[:-1])
            else:
                # Fix state_in_x data.
                count = 0
                state_start = None
                seq_lens = None
                for i, seq_len in enumerate(self[SampleBatch.SEQ_LENS]):
                    count += seq_len
                    if count >= end:
                        state_idx = 0
                        state_key = "state_in_{}".format(state_idx)
                        if state_start is None:
                            state_start = i
                        while state_key in self:
                            data[state_key] = self[state_key][state_start : i + 1]
                            state_idx += 1
                            state_key = "state_in_{}".format(state_idx)
                        seq_lens = list(self[SampleBatch.SEQ_LENS][state_start:i]) + [
                            seq_len - (count - end)
                        ]
                        if start < 0:
                            seq_lens[0] += -start
                        diff = sum(seq_lens) - (end - start)
                        if diff > 0:
                            seq_lens[0] -= diff
                        assert sum(seq_lens) == (end - start)
                        break
                    elif state_start is None and count > start:
                        state_start = i

            return SampleBatch(
                data,
                seq_lens=seq_lens,
                _is_training=self.is_training,
                _time_major=self.time_major,
                _num_grad_updates=self.num_grad_updates,
            )
        else:
            return SampleBatch(
                tree.map_structure(lambda value: value[start:end], self),
                _is_training=self.is_training,
                _time_major=self.time_major,
                _num_grad_updates=self.num_grad_updates,
            )

    def _batch_slice(self, slice_: slice) -> "SampleBatch":
        """Helper method to handle SampleBatch slicing using a slice object.

        The returned SampleBatch uses the same underlying data object as
        `self`, so changing the slice will also change `self`.

        Note that only zero or positive bounds are allowed for both start
        and stop values. The slice step must be 1 (or None, which is the
        same).

        Args:
            slice_: The python slice object to slice by.

        Returns:
            A new SampleBatch, however "linking" into the same data
            (sliced) as self.
        """
        start = slice_.start or 0
        stop = slice_.stop or len(self[SampleBatch.SEQ_LENS])
        # If stop goes beyond the length of this batch -> Make it go till the
        # end only (including last item).
        # Analogous to `l = [0, 1, 2]; l[:100] -> [0, 1, 2];`.
        if stop > len(self):
            stop = len(self)
        assert start >= 0 and stop >= 0 and slice_.step in [1, None]

        data = tree.map_structure(lambda value: value[start:stop], self)
        return SampleBatch(
            data,
            _is_training=self.is_training,
            _time_major=self.time_major,
            _num_grad_updates=self.num_grad_updates,
        )

    @PublicAPI
    def timeslices(
        self,
        size: Optional[int] = None,
        num_slices: Optional[int] = None,
        k: Optional[int] = None,
    ) -> List["SampleBatch"]:
        """Returns SampleBatches, each one representing a k-slice of this one.

        Will start from timestep 0 and produce slices of size=k.

        Args:
            size: The size (in timesteps) of each returned SampleBatch.
            num_slices: The number of slices to produce.
            k: Deprecated: Use size or num_slices instead. The size
                (in timesteps) of each returned SampleBatch.

        Returns:
            The list of `num_slices` (new) SampleBatches or n (new)
            SampleBatches each one of size `size`.
        """
        if size is None and num_slices is None:
            deprecation_warning("k", "size or num_slices")
            assert k is not None
            size = k

        if size is None:
            assert isinstance(num_slices, int)

            slices = []
            left = len(self)
            start = 0
            while left:
                len_ = left // (num_slices - len(slices))
                stop = start + len_
                slices.append(self[start:stop])
                left -= len_
                start = stop

            return slices

        else:
            assert isinstance(size, int)

            slices = []
            left = len(self)
            start = 0
            while left:
                stop = start + size
                slices.append(self[start:stop])
                left -= size
                start = stop

            return slices

    @Deprecated(new="SampleBatch.right_zero_pad", error=False)
    def zero_pad(self, max_seq_len, exclude_states=True):
        return self.right_zero_pad(max_seq_len, exclude_states)

    def right_zero_pad(self, max_seq_len: int, exclude_states: bool = True):
        """Right (adding zeros at end) zero-pads this SampleBatch in-place.

        This will set the `self.zero_padded` flag to True and
        `self.max_seq_len` to the given `max_seq_len` value.

        Args:
            max_seq_len: The max (total) length to zero pad to.
            exclude_states: If False, also right-zero-pad all
                `state_in_x` data. If True, leave `state_in_x` keys
                as-is.

        Returns:
            This very (now right-zero-padded) SampleBatch.

        Raises:
            ValueError: If self[SampleBatch.SEQ_LENS] is None (not defined).

        Examples:
            >>> from ray.rllib.policy.sample_batch import SampleBatch
            >>> batch = SampleBatch( # doctest: +SKIP
            ...     {"a": [1, 2, 3], "seq_lens": [1, 2]})
            >>> print(batch.right_zero_pad(max_seq_len=4)) # doctest: +SKIP
            {"a": [1, 0, 0, 0, 2, 3, 0, 0], "seq_lens": [1, 2]}

            >>> batch = SampleBatch({"a": [1, 2, 3], # doctest: +SKIP
            ...                      "state_in_0": [1.0, 3.0],
            ...                      "seq_lens": [1, 2]})
            >>> print(batch.right_zero_pad(max_seq_len=5)) # doctest: +SKIP
            {"a": [1, 0, 0, 0, 0, 2, 3, 0, 0, 0],
             "state_in_0": [1.0, 3.0],  # <- all state-ins remain as-is
             "seq_lens": [1, 2]}
        """
        seq_lens = self.get(SampleBatch.SEQ_LENS)
        if seq_lens is None:
            raise ValueError(
                "Cannot right-zero-pad SampleBatch if no `seq_lens` field "
                f"present! SampleBatch={self}"
            )

        length = len(seq_lens) * max_seq_len

        def _zero_pad_in_place(path, value):
            # Skip "state_in_..." columns and "seq_lens".
            if (exclude_states is True and path[0].startswith("state_in_")) or path[
                0
            ] == SampleBatch.SEQ_LENS:
                return
            # Generate zero-filled primer of len=max_seq_len.
            if value.dtype == object or value.dtype.type is np.str_:
                f_pad = [None] * length
            else:
                # Make sure type doesn't change.
                f_pad = np.zeros((length,) + np.shape(value)[1:], dtype=value.dtype)
            # Fill primer with data.
            f_pad_base = f_base = 0
            for len_ in self[SampleBatch.SEQ_LENS]:
                f_pad[f_pad_base : f_pad_base + len_] = value[f_base : f_base + len_]
                f_pad_base += max_seq_len
                f_base += len_
            assert f_base == len(value), value

            # Update our data in-place.
            curr = self
            for i, p in enumerate(path):
                if i == len(path) - 1:
                    curr[p] = f_pad
                curr = curr[p]

        self_as_dict = {k: v for k, v in self.items()}
        tree.map_structure_with_path(_zero_pad_in_place, self_as_dict)

        # Set flags to indicate, we are now zero-padded (and to what extend).
        self.zero_padded = True
        self.max_seq_len = max_seq_len

        return self

    @ExperimentalAPI
    def to_device(self, device, framework="torch"):
        """TODO: transfer batch to given device as framework tensor."""
        if framework == "torch":
            assert torch is not None
            for k, v in self.items():
                self[k] = convert_to_torch_tensor(v, device)
        else:
            raise NotImplementedError
        return self

    @PublicAPI
    def size_bytes(self) -> int:
        """Returns sum over number of bytes of all data buffers.

        For numpy arrays, we use ``.nbytes``. For all other value types, we use
        sys.getsizeof(...).

        Returns:
            The overall size in bytes of the data buffer (all columns).
        """
        return sum(
            v.nbytes if isinstance(v, np.ndarray) else sys.getsizeof(v)
            for v in tree.flatten(self)
        )

    def get(self, key, default=None):
        """Returns one column (by key) from the data or a default value."""
        try:
            return self.__getitem__(key)
        except KeyError:
            return default

    @PublicAPI
    def as_multi_agent(self) -> "MultiAgentBatch":
        """Returns the respective MultiAgentBatch using DEFAULT_POLICY_ID.

        Returns:
            The MultiAgentBatch (using DEFAULT_POLICY_ID) corresponding
            to this SampleBatch.
        """
        return MultiAgentBatch({DEFAULT_POLICY_ID: self}, self.count)

    @PublicAPI
    def __getitem__(self, key: Union[str, slice]) -> TensorType:
        """Returns one column (by key) from the data or a sliced new batch.

        Args:
            key: The key (column name) to return or
                a slice object for slicing this SampleBatch.

        Returns:
            The data under the given key or a sliced version of this batch.
        """
        if isinstance(key, slice):
            return self._slice(key)

        # Special key DONES -> Translate to `TERMINATEDS | TRUNCATEDS` to reflect
        # the old meaning of DONES.
        if key == SampleBatch.DONES:
            return self[SampleBatch.TERMINATEDS]
        # Backward compatibility for when "input-dicts" were used.
        elif key == "is_training":
            if log_once("SampleBatch['is_training']"):
                deprecation_warning(
                    old="SampleBatch['is_training']",
                    new="SampleBatch.is_training",
                    error=False,
                )
            return self.is_training

        if not hasattr(self, key) and key in self:
            self.accessed_keys.add(key)

        value = dict.__getitem__(self, key)
        if self.get_interceptor is not None:
            if key not in self.intercepted_values:
                self.intercepted_values[key] = self.get_interceptor(value)
            value = self.intercepted_values[key]
        return value

    @PublicAPI
    def __setitem__(self, key, item) -> None:
        """Inserts (overrides) an entire column (by key) in the data buffer.

        Args:
            key: The column name to set a value for.
            item: The data to insert.
        """
        # Disallow setting DONES key directly.
        if key == SampleBatch.DONES:
            raise KeyError(
                "Cannot set `DONES` anymore in a SampleBatch! "
                "Instead, set the new TERMINATEDS and TRUNCATEDS keys. The values under"
                " DONES will then be automatically computed using terminated|truncated."
            )
        # Defend against creating SampleBatch via pickle (no property
        # `added_keys` and first item is already set).
        elif not hasattr(self, "added_keys"):
            dict.__setitem__(self, key, item)
            return

        # Backward compatibility for when "input-dicts" were used.
        if key == "is_training":
            if log_once("SampleBatch['is_training']"):
                deprecation_warning(
                    old="SampleBatch['is_training']",
                    new="SampleBatch.is_training",
                    error=False,
                )
            self._is_training = item
            return

        if key not in self:
            self.added_keys.add(key)

        dict.__setitem__(self, key, item)
        if key in self.intercepted_values:
            self.intercepted_values[key] = item

    @property
    def is_training(self):
        if self.get_interceptor is not None and isinstance(self._is_training, bool):
            if "_is_training" not in self.intercepted_values:
                self.intercepted_values["_is_training"] = self.get_interceptor(
                    self._is_training
                )
            return self.intercepted_values["_is_training"]
        return self._is_training

    def set_training(self, training: Union[bool, "tf1.placeholder"] = True):
        """Sets the `is_training` flag for this SampleBatch."""
        self._is_training = training
        self.intercepted_values.pop("_is_training", None)

    @PublicAPI
    def __delitem__(self, key):
        self.deleted_keys.add(key)
        dict.__delitem__(self, key)

    @DeveloperAPI
    def compress(
        self, bulk: bool = False, columns: Set[str] = frozenset(["obs", "new_obs"])
    ) -> "SampleBatch":
        """Compresses the data buffers (by column) in place.

        Args:
            bulk: Whether to compress across the batch dimension (0)
                as well. If False will compress n separate list items, where n
                is the batch size.
            columns: The columns to compress. Default: Only
                compress the obs and new_obs columns.

        Returns:
            This very (now compressed) SampleBatch.
        """

        def _compress_in_place(path, value):
            if path[0] not in columns:
                return
            curr = self
            for i, p in enumerate(path):
                if i == len(path) - 1:
                    if bulk:
                        curr[p] = pack(value)
                    else:
                        curr[p] = np.array([pack(o) for o in value])
                curr = curr[p]

        tree.map_structure_with_path(_compress_in_place, self)

        return self

    @DeveloperAPI
    def decompress_if_needed(
        self, columns: Set[str] = frozenset(["obs", "new_obs"])
    ) -> "SampleBatch":
        """Decompresses data buffers (per column if not compressed) in place.

        Args:
            columns: The columns to decompress. Default: Only
                decompress the obs and new_obs columns.

        Returns:
            This very (now uncompressed) SampleBatch.
        """

        def _decompress_in_place(path, value):
            if path[0] not in columns:
                return
            curr = self
            for p in path[:-1]:
                curr = curr[p]
            # Bulk compressed.
            if is_compressed(value):
                curr[path[-1]] = unpack(value)
            # Non bulk compressed.
            elif len(value) > 0 and is_compressed(value[0]):
                curr[path[-1]] = np.array([unpack(o) for o in value])

        tree.map_structure_with_path(_decompress_in_place, self)

        return self

    @DeveloperAPI
    def set_get_interceptor(self, fn):
        """Sets a function to be called on every getitem."""
        # If get-interceptor changes, must erase old intercepted values.
        if fn is not self.get_interceptor:
            self.intercepted_values = {}
        self.get_interceptor = fn

    def __repr__(self):
        keys = list(self.keys())
        if self.get(SampleBatch.SEQ_LENS) is None:
            return f"SampleBatch({self.count}: {keys})"
        else:
            keys.remove(SampleBatch.SEQ_LENS)
            return (
                f"SampleBatch({self.count} " f"(seqs={len(self['seq_lens'])}): {keys})"
            )

    def _slice(self, slice_: slice) -> "SampleBatch":
        """Helper method to handle SampleBatch slicing using a slice object.

        The returned SampleBatch uses the same underlying data object as
        `self`, so changing the slice will also change `self`.

        Note that only zero or positive bounds are allowed for both start
        and stop values. The slice step must be 1 (or None, which is the
        same).

        Args:
            slice_: The python slice object to slice by.

        Returns:
            A new SampleBatch, however "linking" into the same data
            (sliced) as self.
        """
        if self._slice_seq_lens_in_B:
            return self._batch_slice(slice_)

        start = slice_.start or 0
        stop = slice_.stop or len(self)
        # If stop goes beyond the length of this batch -> Make it go till the
        # end only (including last item).
        # Analogous to `l = [0, 1, 2]; l[:100] -> [0, 1, 2];`.
        if stop > len(self):
            stop = len(self)

        if (
            self.get(SampleBatch.SEQ_LENS) is not None
            and len(self[SampleBatch.SEQ_LENS]) > 0
        ):
            # Build our slice-map, if not done already.
            if not self._slice_map:
                sum_ = 0
                for i, l in enumerate(map(int, self[SampleBatch.SEQ_LENS])):
                    self._slice_map.extend([(i, sum_)] * l)
                    sum_ = sum_ + l
                # In case `stop` points to the very end (lengths of this
                # batch), return the last sequence (the -1 here makes sure we
                # never go beyond it; would result in an index error below).
                self._slice_map.append((len(self[SampleBatch.SEQ_LENS]), sum_))

            start_seq_len, start_unpadded = self._slice_map[start]
            stop_seq_len, stop_unpadded = self._slice_map[stop]
            start_padded = start_unpadded
            stop_padded = stop_unpadded
            if self.zero_padded:
                start_padded = start_seq_len * self.max_seq_len
                stop_padded = stop_seq_len * self.max_seq_len

            def map_(path, value):
                if path[0] != SampleBatch.SEQ_LENS and not path[0].startswith(
                    "state_in_"
                ):
                    if path[0] != SampleBatch.INFOS:
                        return value[start_padded:stop_padded]
                    else:
                        if (
                            (isinstance(value, np.ndarray) and value.size > 0)
                            or (
                                torch
                                and torch.is_tensor(value)
                                and len(list(value.shape)) > 0
                            )
                            or (tf and tf.is_tensor(value) and tf.size(value) > 0)
                        ):
                            return value[start_unpadded:stop_unpadded]
                        else:
                            # Since infos should be stored as lists and not arrays,
                            # we return the values here and slice them separately
                            # TODO(Artur): Clean this hack up.
                            return value
                else:
                    return value[start_seq_len:stop_seq_len]

            data = tree.map_structure_with_path(map_, self)

            # Since we don't slice in the above map_ function, we do it here.
            if isinstance(data.get(SampleBatch.INFOS), list):
                data[SampleBatch.INFOS] = data[SampleBatch.INFOS][
                    start_unpadded:stop_unpadded
                ]

            return SampleBatch(
                data,
                _is_training=self.is_training,
                _time_major=self.time_major,
                _zero_padded=self.zero_padded,
                _max_seq_len=self.max_seq_len if self.zero_padded else None,
                _num_grad_updates=self.num_grad_updates,
            )
        else:

            def map_(value):
                if (
                    isinstance(value, np.ndarray)
                    or (torch and torch.is_tensor(value))
                    or (tf and tf.is_tensor(value))
                ):
                    return value[start:stop]
                else:
                    # Since infos should be stored as lists and not arrays,
                    # we return the values here and slice them separately
                    # TODO(Artur): Clean this hack up.
                    return value

            data = tree.map_structure(map_, self)

            return SampleBatch(
                data,
                _is_training=self.is_training,
                _time_major=self.time_major,
                _num_grad_updates=self.num_grad_updates,
            )

    @Deprecated(error=False)
    def _get_slice_indices(self, slice_size):
        data_slices = []
        data_slices_states = []
        if (
            self.get(SampleBatch.SEQ_LENS) is not None
            and len(self[SampleBatch.SEQ_LENS]) > 0
        ):
            assert np.all(self[SampleBatch.SEQ_LENS] < slice_size), (
                "ERROR: `slice_size` must be larger than the max. seq-len "
                "in the batch!"
            )
            start_pos = 0
            current_slize_size = 0
            actual_slice_idx = 0
            start_idx = 0
            idx = 0
            while idx < len(self[SampleBatch.SEQ_LENS]):
                seq_len = self[SampleBatch.SEQ_LENS][idx]
                current_slize_size += seq_len
                actual_slice_idx += (
                    seq_len if not self.zero_padded else self.max_seq_len
                )
                # Complete minibatch -> Append to data_slices.
                if current_slize_size >= slice_size:
                    end_idx = idx + 1
                    # We are not zero-padded yet; all sequences are
                    # back-to-back.
                    if not self.zero_padded:
                        data_slices.append((start_pos, start_pos + slice_size))
                        start_pos += slice_size
                        if current_slize_size > slice_size:
                            overhead = current_slize_size - slice_size
                            start_pos -= seq_len - overhead
                            idx -= 1
                    # We are already zero-padded: Cut in chunks of max_seq_len.
                    else:
                        data_slices.append((start_pos, actual_slice_idx))
                        start_pos = actual_slice_idx

                    data_slices_states.append((start_idx, end_idx))
                    current_slize_size = 0
                    start_idx = idx + 1
                idx += 1
        else:
            i = 0
            while i < self.count:
                data_slices.append((i, i + slice_size))
                i += slice_size
        return data_slices, data_slices_states

    @ExperimentalAPI
    def get_single_step_input_dict(
        self,
        view_requirements: ViewRequirementsDict,
        index: Union[str, int] = "last",
    ) -> "SampleBatch":
        """Creates single ts SampleBatch at given index from `self`.

        For usage as input-dict for model (action or value function) calls.

        Args:
            view_requirements: A view requirements dict from the model for
                which to produce the input_dict.
            index: An integer index value indicating the
                position in the trajectory for which to generate the
                compute_actions input dict. Set to "last" to generate the dict
                at the very end of the trajectory (e.g. for value estimation).
                Note that "last" is different from -1, as "last" will use the
                final NEXT_OBS as observation input.

        Returns:
            The (single-timestep) input dict for ModelV2 calls.
        """
        last_mappings = {
            SampleBatch.OBS: SampleBatch.NEXT_OBS,
            SampleBatch.PREV_ACTIONS: SampleBatch.ACTIONS,
            SampleBatch.PREV_REWARDS: SampleBatch.REWARDS,
        }

        input_dict = {}
        for view_col, view_req in view_requirements.items():
            if view_req.used_for_compute_actions is False:
                continue

            # Create batches of size 1 (single-agent input-dict).
            data_col = view_req.data_col or view_col
            if index == "last":
                data_col = last_mappings.get(data_col, data_col)
                # Range needed.
                if view_req.shift_from is not None:
                    # Batch repeat value > 1: We have single frames in the
                    # batch at each timestep (for the `data_col`).
                    data = self[view_col][-1]
                    traj_len = len(self[data_col])
                    missing_at_end = traj_len % view_req.batch_repeat_value
                    # Index into the observations column must be shifted by
                    # -1 b/c index=0 for observations means the current (last
                    # seen) observation (after having taken an action).
                    obs_shift = (
                        -1 if data_col in [SampleBatch.OBS, SampleBatch.NEXT_OBS] else 0
                    )
                    from_ = view_req.shift_from + obs_shift
                    to_ = view_req.shift_to + obs_shift + 1
                    if to_ == 0:
                        to_ = None
                    input_dict[view_col] = np.array(
                        [
                            np.concatenate([data, self[data_col][-missing_at_end:]])[
                                from_:to_
                            ]
                        ]
                    )
                # Single index.
                else:
                    input_dict[view_col] = tree.map_structure(
                        lambda v: v[-1:],  # keep as array (w/ 1 element)
                        self[data_col],
                    )
            # Single index somewhere inside the trajectory (non-last).
            else:
                input_dict[view_col] = self[data_col][
                    index : index + 1 if index != -1 else None
                ]

        return SampleBatch(input_dict, seq_lens=np.array([1], dtype=np.int32))


@PublicAPI
class MultiAgentBatch:
    """A batch of experiences from multiple agents in the environment.

    Attributes:
        policy_batches (Dict[PolicyID, SampleBatch]): Mapping from policy
            ids to SampleBatches of experiences.
        count: The number of env steps in this batch.
    """

    @PublicAPI
    def __init__(self, policy_batches: Dict[PolicyID, SampleBatch], env_steps: int):
        """Initialize a MultiAgentBatch instance.

        Args:
            policy_batches: Mapping from policy
                ids to SampleBatches of experiences.
            env_steps: The number of environment steps in the environment
                this batch contains. This will be less than the number of
                transitions this batch contains across all policies in total.
        """

        for v in policy_batches.values():
            assert isinstance(v, SampleBatch)
        self.policy_batches = policy_batches
        # Called "count" for uniformity with SampleBatch.
        # Prefer to access this via the `env_steps()` method when possible
        # for clarity.
        self.count = env_steps

    @PublicAPI
    def env_steps(self) -> int:
        """The number of env steps (there are >= 1 agent steps per env step).

        Returns:
            The number of environment steps contained in this batch.
        """
        return self.count

    @PublicAPI
    def __len__(self) -> int:
        """Same as `self.env_steps()`."""
        return self.count

    @PublicAPI
    def agent_steps(self) -> int:
        """The number of agent steps (there are >= 1 agent steps per env step).

        Returns:
            The number of agent steps total in this batch.
        """
        ct = 0
        for batch in self.policy_batches.values():
            ct += batch.count
        return ct

    @PublicAPI
    def timeslices(self, k: int) -> List["MultiAgentBatch"]:
        """Returns k-step batches holding data for each agent at those steps.

        For examples, suppose we have agent1 observations [a1t1, a1t2, a1t3],
        for agent2, [a2t1, a2t3], and for agent3, [a3t3] only.

        Calling timeslices(1) would return three MultiAgentBatches containing
        [a1t1, a2t1], [a1t2], and [a1t3, a2t3, a3t3].

        Calling timeslices(2) would return two MultiAgentBatches containing
        [a1t1, a1t2, a2t1], and [a1t3, a2t3, a3t3].

        This method is used to implement "lockstep" replay mode. Note that this
        method does not guarantee each batch contains only data from a single
        unroll. Batches might contain data from multiple different envs.
        """
        from ray.rllib.evaluation.sample_batch_builder import SampleBatchBuilder

        # Build a sorted set of (eps_id, t, policy_id, data...)
        steps = []
        for policy_id, batch in self.policy_batches.items():
            for row in batch.rows():
                steps.append(
                    (
                        row[SampleBatch.EPS_ID],
                        row[SampleBatch.T],
                        row[SampleBatch.AGENT_INDEX],
                        policy_id,
                        row,
                    )
                )
        steps.sort()

        finished_slices = []
        cur_slice = collections.defaultdict(SampleBatchBuilder)
        cur_slice_size = 0

        def finish_slice():
            nonlocal cur_slice_size
            assert cur_slice_size > 0
            batch = MultiAgentBatch(
                {k: v.build_and_reset() for k, v in cur_slice.items()}, cur_slice_size
            )
            cur_slice_size = 0
            cur_slice.clear()
            finished_slices.append(batch)

        # For each unique env timestep.
        for _, group in itertools.groupby(steps, lambda x: x[:2]):
            # Accumulate into the current slice.
            for _, _, _, policy_id, row in group:
                cur_slice[policy_id].add_values(**row)
            cur_slice_size += 1
            # Slice has reached target number of env steps.
            if cur_slice_size >= k:
                finish_slice()
                assert cur_slice_size == 0

        if cur_slice_size > 0:
            finish_slice()

        assert len(finished_slices) > 0, finished_slices
        return finished_slices

    @staticmethod
    @PublicAPI
    def wrap_as_needed(
        policy_batches: Dict[PolicyID, SampleBatch], env_steps: int
    ) -> Union[SampleBatch, "MultiAgentBatch"]:
        """Returns SampleBatch or MultiAgentBatch, depending on given policies.
        If policy_batches is empty (i.e. {}) it returns an empty MultiAgentBatch.

        Args:
            policy_batches: Mapping from policy ids to SampleBatch.
            env_steps: Number of env steps in the batch.

        Returns:
            The single default policy's SampleBatch or a MultiAgentBatch
            (more than one policy).
        """
        if len(policy_batches) == 1 and DEFAULT_POLICY_ID in policy_batches:
            return policy_batches[DEFAULT_POLICY_ID]
        return MultiAgentBatch(policy_batches=policy_batches, env_steps=env_steps)

    @staticmethod
    @PublicAPI
    @Deprecated(new="concat_samples() from rllib.policy.sample_batch", error=True)
    def concat_samples(samples: List["MultiAgentBatch"]) -> "MultiAgentBatch":
        return concat_samples_into_ma_batch(samples)

    @PublicAPI
    def copy(self) -> "MultiAgentBatch":
        """Deep-copies self into a new MultiAgentBatch.

        Returns:
            The copy of self with deep-copied data.
        """
        return MultiAgentBatch(
            {k: v.copy() for (k, v) in self.policy_batches.items()}, self.count
        )

    @PublicAPI
    def size_bytes(self) -> int:
        """
        Returns:
            The overall size in bytes of all policy batches (all columns).
        """
        return sum(b.size_bytes() for b in self.policy_batches.values())

    @DeveloperAPI
    def compress(
        self, bulk: bool = False, columns: Set[str] = frozenset(["obs", "new_obs"])
    ) -> None:
        """Compresses each policy batch (per column) in place.

        Args:
            bulk: Whether to compress across the batch dimension (0)
                as well. If False will compress n separate list items, where n
                is the batch size.
            columns: Set of column names to compress.
        """
        for batch in self.policy_batches.values():
            batch.compress(bulk=bulk, columns=columns)

    @DeveloperAPI
    def decompress_if_needed(
        self, columns: Set[str] = frozenset(["obs", "new_obs"])
    ) -> "MultiAgentBatch":
        """Decompresses each policy batch (per column), if already compressed.

        Args:
            columns: Set of column names to decompress.

        Returns:
            Self.
        """
        for batch in self.policy_batches.values():
            batch.decompress_if_needed(columns)
        return self

    @DeveloperAPI
    def as_multi_agent(self) -> "MultiAgentBatch":
        """Simply returns `self` (already a MultiAgentBatch).

        Returns:
            This very instance of MultiAgentBatch.
        """
        return self

    def __getitem__(self, key: str) -> SampleBatch:
        """Returns the SampleBatch for the given policy id."""
        return self.policy_batches[key]

    def __str__(self):
        return "MultiAgentBatch({}, env_steps={})".format(
            str(self.policy_batches), self.count
        )

    def __repr__(self):
        return "MultiAgentBatch({}, env_steps={})".format(
            str(self.policy_batches), self.count
        )


@PublicAPI
def concat_samples(samples: List[SampleBatchType]) -> SampleBatchType:
    """Concatenates a list of  SampleBatches or MultiAgentBatches.

    If all items in the list are or SampleBatch typ4, the output will be
    a SampleBatch type. Otherwise, the output will be a MultiAgentBatch type.
    If input is a mixture of SampleBatch and MultiAgentBatch types, it will treat
    SampleBatch objects as MultiAgentBatch types with 'default_policy' key and
    concatenate it with th rest of MultiAgentBatch objects.
    Empty samples are simply ignored.

    Args:
        samples: List of SampleBatches or MultiAgentBatches to be
            concatenated.

    Returns:
        A new (concatenated) SampleBatch or MultiAgentBatch.

    Examples:
        >>> import numpy as np
        >>> from ray.rllib.policy.sample_batch import SampleBatch
        >>> b1 = SampleBatch({"a": np.array([1, 2]), # doctest: +SKIP
        ...                   "b": np.array([10, 11])})
        >>> b2 = SampleBatch({"a": np.array([3]), # doctest: +SKIP
        ...                   "b": np.array([12])})
        >>> print(concat_samples([b1, b2])) # doctest: +SKIP
        {"a": np.array([1, 2, 3]), "b": np.array([10, 11, 12])}

        >>> c1 = MultiAgentBatch({'default_policy': { # doctest: +SKIP
        ...                                 "a": np.array([1, 2]),
        ...                                 "b": np.array([10, 11])
        ...                                 }}, env_steps=2)
        >>> c2 = SampleBatch({"a": np.array([3]), # doctest: +SKIP
        ...                   "b": np.array([12])})
        >>> print(concat_samples([b1, b2])) # doctest: +SKIP
        MultiAgentBatch = {'default_policy': {"a": np.array([1, 2, 3]),
                                              "b": np.array([10, 11, 12])}}
    """

    if any(isinstance(s, MultiAgentBatch) for s in samples):
        return concat_samples_into_ma_batch(samples)

    # the output is a SampleBatch type
    concatd_seq_lens = []
    concatd_num_grad_updates = [0, 0.0]  # [0]=count; [1]=weighted sum values
    concated_samples = []
    # Make sure these settings are consistent amongst all batches.
    zero_padded = max_seq_len = time_major = None
    for s in samples:
        if s.count > 0:
            if max_seq_len is None:
                zero_padded = s.zero_padded
                max_seq_len = s.max_seq_len
                time_major = s.time_major

            # Make sure these settings are consistent amongst all batches.
            if s.zero_padded != zero_padded or s.time_major != time_major:
                raise ValueError(
                    "All SampleBatches' `zero_padded` and `time_major` settings "
                    "must be consistent!"
                )
            if (
                s.max_seq_len is None or max_seq_len is None
            ) and s.max_seq_len != max_seq_len:
                raise ValueError(
                    "Samples must consistently either provide or omit " "`max_seq_len`!"
                )
            elif zero_padded and s.max_seq_len != max_seq_len:
                raise ValueError(
                    "For `zero_padded` SampleBatches, the values of `max_seq_len` "
                    "must be consistent!"
                )

            if max_seq_len is not None:
                max_seq_len = max(max_seq_len, s.max_seq_len)
            if s.get(SampleBatch.SEQ_LENS) is not None:
                concatd_seq_lens.extend(s[SampleBatch.SEQ_LENS])
            if s.num_grad_updates is not None:
                concatd_num_grad_updates[0] += s.count
                concatd_num_grad_updates[1] += s.num_grad_updates * s.count

            concated_samples.append(s)

    # If we don't have any samples (0 or only empty SampleBatches),
    # return an empty SampleBatch here.
    if len(concated_samples) == 0:
        return SampleBatch()

    # Collect the concat'd data.
    concatd_data = {}

    for k in concated_samples[0].keys():
        try:
            if k == "infos":
                concatd_data[k] = _concat_values(
                    *[s[k] for s in concated_samples],
                    time_major=time_major,
                )
            else:
                values_to_concat = [c[k] for c in concated_samples]
                _concat_values_w_time = partial(_concat_values, time_major=time_major)
                concatd_data[k] = tree.map_structure(
                    _concat_values_w_time, *values_to_concat
                )
        except RuntimeError as e:
            # This should catch torch errors that occur when concatenating
            # tensors from different devices.
            raise e
        except Exception as e:
            # Other errors are likely due to mismatching sub-structures.
            raise ValueError(
                f"Cannot concat data under key '{k}', b/c "
                "sub-structures under that key don't match. "
                f"`samples`={samples}\n Original error: \n {e}"
            )

    if concatd_seq_lens != [] and torch and torch.is_tensor(concatd_seq_lens[0]):
        concatd_seq_lens = torch.Tensor(concatd_seq_lens)
    elif concatd_seq_lens != [] and tf and tf.is_tensor(concatd_seq_lens[0]):
        concatd_seq_lens = tf.convert_to_tensor(concatd_seq_lens)

    # Return a new (concat'd) SampleBatch.
    return SampleBatch(
        concatd_data,
        seq_lens=concatd_seq_lens,
        _time_major=time_major,
        _zero_padded=zero_padded,
        _max_seq_len=max_seq_len,
        # Compute weighted average of the num_grad_updates for the batches
        # (assuming they all come from the same policy).
        _num_grad_updates=(
            concatd_num_grad_updates[1] / (concatd_num_grad_updates[0] or 1.0)
        ),
    )


@PublicAPI
def concat_samples_into_ma_batch(samples: List[SampleBatchType]) -> "MultiAgentBatch":
    """Concatenates a list of SampleBatchTypes to a single MultiAgentBatch type.

    This function, as opposed to concat_samples() forces the output to always be
    MultiAgentBatch which is more generic than SampleBatch.

    Args:
        samples: List of SampleBatches or MultiAgentBatches to be
            concatenated.

    Returns:
        A new (concatenated) MultiAgentBatch.

    Examples:
        >>> import numpy as np
        >>> from ray.rllib.policy.sample_batch import SampleBatch
        >>> b1 = MultiAgentBatch({'default_policy': { # doctest: +SKIP
        ...                                 "a": np.array([1, 2]),
        ...                                 "b": np.array([10, 11])
        ...                                 }}, env_steps=2)
        >>> b2 = SampleBatch({"a": np.array([3]), # doctest: +SKIP
        ...                   "b": np.array([12])})
        >>> print(concat_samples([b1, b2])) # doctest: +SKIP
        MultiAgentBatch = {'default_policy': {"a": np.array([1, 2, 3]),
                                              "b": np.array([10, 11, 12])}}

    """

    policy_batches = collections.defaultdict(list)
    env_steps = 0
    for s in samples:
        # Some batches in `samples` may be SampleBatch.
        if isinstance(s, SampleBatch):
            # If empty SampleBatch: ok (just ignore).
            if len(s) <= 0:
                continue
            else:
                # if non-empty: just convert to MA-batch and move forward
                s = s.as_multi_agent()
        elif not isinstance(s, MultiAgentBatch):
            # Otherwise: Error.
            raise ValueError(
                "`concat_samples_into_ma_batch` can only concat "
                "SampleBatch|MultiAgentBatch objects, not {}!".format(type(s).__name__)
            )

        for key, batch in s.policy_batches.items():
            policy_batches[key].append(batch)
        env_steps += s.env_steps()

    out = {}
    for key, batches in policy_batches.items():
        out[key] = concat_samples(batches)

    return MultiAgentBatch(out, env_steps)


def _concat_values(*values, time_major=None) -> TensorType:
    """Concatenates a list of values.

    Args:
        values: The values to concatenate.
        time_major: Whether to concatenate along the first axis
            (time_major=False) or the second axis (time_major=True).
    """
    if torch and torch.is_tensor(values[0]):
        return torch.cat(values, dim=1 if time_major else 0)
    elif isinstance(values[0], np.ndarray):
        return np.concatenate(values, axis=1 if time_major else 0)
    elif tf and tf.is_tensor(values[0]):
        return tf.concat(values, axis=1 if time_major else 0)
    elif isinstance(values[0], list):
        concatenated_list = []
        for sublist in values:
            concatenated_list.extend(sublist)
        return concatenated_list
    else:
        raise ValueError(
            f"Unsupported type for concatenation: {type(values[0])} "
            f"first element: {values[0]}"
        )


@DeveloperAPI
def convert_ma_batch_to_sample_batch(batch: SampleBatchType) -> SampleBatch:
    """Converts a MultiAgentBatch to a SampleBatch if neccessary.

    Args:
        batch: The SampleBatchType to convert.

    Returns:
        batch: the converted SampleBatch

    Raises:
        ValueError if the MultiAgentBatch has more than one policy_id
        or if the policy_id is not `DEFAULT_POLICY_ID`
    """
    if isinstance(batch, MultiAgentBatch):
        policy_keys = batch.policy_batches.keys()
        if len(policy_keys) == 1 and DEFAULT_POLICY_ID in policy_keys:
            batch = batch.policy_batches[DEFAULT_POLICY_ID]
        else:
            raise ValueError(
                "RLlib tried to convert a multi agent-batch with data from more "
                "than one policy to a single-agent batch. This is not supported and "
                "may be due to a number of issues. Here are two possible ones:"
                "1) Off-Policy Estimation is not implemented for "
                "multi-agent batches. You can set `off_policy_estimation_methods: {}` "
                "to resolve this."
                "2) Loading multi-agent data for offline training is not implemented."
                "Load single-agent data instead to resolve this."
            )
    return batch
