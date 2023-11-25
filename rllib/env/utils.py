import logging
from typing import List, Optional, Type, Union

import gymnasium as gym
import numpy as np
import tree  # pip install dm_tree

from ray.rllib.env.env_context import EnvContext
from ray.rllib.env.multi_agent_env import MultiAgentEnv
from ray.rllib.env.wrappers.multi_agent_env_compatibility import (
    MultiAgentEnvCompatibility,
)
from ray.rllib.utils.error import (
    ERR_MSG_INVALID_ENV_DESCRIPTOR,
    ERR_MSG_OLD_GYM_API,
    EnvError,
)
from ray.rllib.utils.gym import check_old_gym_env
from ray.rllib.utils.numpy import one_hot, one_hot_multidiscrete
from ray.rllib.utils.spaces.space_utils import (
    batch,
    get_dummy_batch_for_space,
    get_base_struct_from_space,
)
from ray.util import log_once
from ray.util.annotations import PublicAPI


logger = logging.getLogger(__name__)


@PublicAPI
def try_import_pyspiel(error: bool = False):
    """Tries importing pyspiel and returns the module (or None).

    Args:
        error: Whether to raise an error if pyspiel cannot be imported.

    Returns:
        The pyspiel module.

    Raises:
        ImportError: If error=True and pyspiel is not installed.
    """
    try:
        import pyspiel

        return pyspiel
    except ImportError:
        if error:
            raise ImportError(
                "Could not import pyspiel! Pygame is not a dependency of RLlib "
                "and RLlib requires you to install pygame separately: "
                "`pip install pygame`."
            )
        return None


@PublicAPI
def try_import_open_spiel(error: bool = False):
    """Tries importing open_spiel and returns the module (or None).

    Args:
        error: Whether to raise an error if open_spiel cannot be imported.

    Returns:
        The open_spiel module.

    Raises:
        ImportError: If error=True and open_spiel is not installed.
    """
    try:
        import open_spiel

        return open_spiel
    except ImportError:
        if error:
            raise ImportError(
                "Could not import open_spiel! open_spiel is not a dependency of RLlib "
                "and RLlib requires you to install open_spiel separately: "
                "`pip install open_spiel`."
            )
        return None


def _gym_env_creator(
    env_context: EnvContext,
    env_descriptor: Union[str, Type[gym.Env]],
    auto_wrap_old_gym_envs: bool = True,
) -> gym.Env:
    """Tries to create a gym env given an EnvContext object and descriptor.

    Note: This function tries to construct the env from a string descriptor
    only using possibly installed RL env packages (such as gym, pybullet_envs,
    vizdoomgym, etc..). These packages are no installation requirements for
    RLlib. In case you would like to support more such env packages, add the
    necessary imports and construction logic below.

    Args:
        env_context: The env context object to configure the env.
            Note that this is a config dict, plus the properties:
            `worker_index`, `vector_index`, and `remote`.
        env_descriptor: The env descriptor as a gym-registered string, e.g. CartPole-v1,
            ALE/MsPacman-v5, VizdoomBasic-v0, or CartPoleContinuousBulletEnv-v0.
            Alternatively, the gym.Env subclass to use.
        auto_wrap_old_gym_envs: Whether to auto-wrap old gym environments (using
            the pre 0.24 gym APIs, e.g. reset() returning single obs and no info
            dict). If True, RLlib will automatically wrap the given gym env class
            with the gym-provided compatibility wrapper (gym.wrappers.EnvCompatibility).
            If False, RLlib will produce a descriptive error on which steps to perform
            to upgrade to gymnasium (or to switch this flag to True).

    Returns:
        The actual gym environment object.

    Raises:
        gym.error.Error: If the env cannot be constructed.
    """
    # Allow for PyBullet or VizdoomGym envs to be used as well
    # (via string). This allows for doing things like
    # `env=CartPoleContinuousBulletEnv-v0` or
    # `env=VizdoomBasic-v0`.
    try:
        import pybullet_envs

        pybullet_envs.getList()
    except (AttributeError, ModuleNotFoundError, ImportError):
        pass
    try:
        import vizdoomgym

        vizdoomgym.__name__  # trick LINTer.
    except (ModuleNotFoundError, ImportError):
        pass

    # Try creating a gym env. If this fails we can output a
    # decent error message.
    try:
        # If class provided, call constructor directly.
        if isinstance(env_descriptor, type):
            env = env_descriptor(env_context)
        else:
            env = gym.make(env_descriptor, **env_context)
        # If we are dealing with an old gym-env API, use the provided compatibility
        # wrapper.
        if auto_wrap_old_gym_envs:
            try:
                # Call the env's reset() method to check for the env using the old
                # gym (reset doesn't take `seed` and `options` args and returns only
                # the initial observations) or new gymnasium APIs (reset takes `seed`
                # and `options` AND returns observations and infos).
                obs_and_infos = env.reset(seed=None, options={})
                # Check return values for correct gymnasium .
                check_old_gym_env(reset_results=obs_and_infos)
            # TypeError for `reset()` not accepting seed/options.
            # ValueError for `check_old_gym_env` raising error if return values
            # incorrect.
            except Exception:
                if log_once("auto_wrap_gym_api"):
                    logger.warning(
                        "`config.auto_wrap_old_gym_envs` is activated AND you seem to "
                        "have provided an old gym-API environment. RLlib will therefore"
                        " try to auto-fix the following error. However, please "
                        "consider switching over to the new `gymnasium` APIs:\n"
                        + ERR_MSG_OLD_GYM_API
                    )
                # Multi-agent case.
                if isinstance(env, MultiAgentEnv):
                    env = MultiAgentEnvCompatibility(env)
                # Single agent (gymnasium.Env) case.
                else:
                    env = gym.wrappers.EnvCompatibility(env)
                # Repeat the checks, now everything should work.
                obs_and_infos = env.reset(seed=None, options={})
                check_old_gym_env(reset_results=obs_and_infos)
    except gym.error.Error:
        raise EnvError(ERR_MSG_INVALID_ENV_DESCRIPTOR.format(env_descriptor))

    return env


class BufferWithInfiniteLookback:
    def __init__(
        self,
        data: Optional[List] = None,
        lookback: int = 0,
        space: Optional[gym.Space] = None,
    ):
        self.data = data if data is not None else []
        self.lookback = lookback
        self.finalized = not isinstance(self.data, list)
        self.space = space
        self.space_struct = get_base_struct_from_space(self.space)

    def append(self, item) -> None:
        """Appends the given item to the end of this buffer."""
        if self.finalized:
            raise RuntimeError(f"Cannot `append` to a finalized {type(self).__name__}.")
        self.data.append(item)

    def extend(self, items):
        """Appends all items in `items` to the end of this buffer."""
        if self.finalized:
            raise RuntimeError(f"Cannot `extend` a finalized {type(self).__name__}.")
        for item in items:
            self.append(item)

    def pop(self, index: int = -1):
        """Removes the item at `index` from this buffer."""
        if self.finalized:
            raise RuntimeError(f"Cannot `pop` from a finalized {type(self).__name__}.")
        return self.data.pop(index)

    def finalize(self):
        """Finalizes this buffer by converting internal data lists into numpy arrays.

        Thereby, if the individual items in the list are complex (nested 2)
        """
        if not self.finalized:
            self.data = batch(self.data)
            self.finalized = True

    def get(
        self,
        indices: Optional[Union[int, slice, List[int]]] = None,
        neg_indices_left_of_zero: bool = False,
        fill: Optional[float] = None,
        one_hot_discrete: bool = False,
    ):
        """TODO: docstring"""
        if fill is not None and self.space is None:
            raise ValueError(
                f"Cannot use `fill` argument in `{type(self).__name__}.get()` if a "
                "gym.Space was NOT provided during construction!"
            )

        if indices is None:
            data = self._get_all_data()
        elif isinstance(indices, slice):
            data = self._get_slice(
                indices,
                fill=fill,
                neg_indices_left_of_zero=neg_indices_left_of_zero,
            )
        elif isinstance(indices, list):
            data = [
                self._get_int_index(
                    idx,
                    fill=fill,
                    neg_indices_left_of_zero=neg_indices_left_of_zero,
                )
                for idx in indices
            ]
            if self.finalized:
                data = batch(data)
        else:
            assert isinstance(indices, int)
            data = self._get_int_index(
                indices,
                fill=fill,
                neg_indices_left_of_zero=neg_indices_left_of_zero,
            )

        # Convert discrete/multi-discrete components to one-hot vectors, if required.
        if one_hot_discrete:
            data = self._one_hot(data)

        return data

    def __getitem__(self, item):
        """Support squared bracket syntax, e.g. buffer[:5]."""
        return self.get(item)

    def __len__(self):
        """Return the length of our data, excluding the lookback buffer."""
        return len(self.data) - self.lookback

    def _get_all_data(self):
        return self._get_slice(slice(None, None))

    def _get_slice(self, slice_, fill=None, neg_indices_left_of_zero=False):
        fill_left_count = fill_right_count = 0

        # Re-interpret slice bounds as absolute positions (>=0) within our
        # internal data.
        start = slice_.start
        stop = slice_.stop

        # Start is None -> Exclude lookback buffer.
        if start is None:
            start = self.lookback
        # Start is negative.
        elif start < 0:
            # `neg_indices_left_of_zero=True` -> User wants to index into the lookback
            # range.
            if neg_indices_left_of_zero:
                start = self.lookback + start
            # Interpret index as counting "from end".
            else:
                start = len(self.data) + start
        # Start is 0 or positive -> timestep right after lookback is interpreted as 0.
        else:
            start = self.lookback + start

        # Stop is None -> Set stop to very last index + 1 of our internal data.
        if stop is None:
            stop = len(self.data)
        # Stop is negative.
        elif stop < 0:
            # `neg_indices_left_of_zero=True` -> User wants to index into the lookback
            # range. Set to 0 (beginning of lookback buffer) if result is a negative
            # index.
            if neg_indices_left_of_zero:
                stop = self.lookback + stop
            # Interpret index as counting "from end". Set to 0 (beginning of actual
            # episode) if result is a negative index.
            else:
                stop = len(self.data) + stop
        # Stop is positive -> Add lookback range to it.
        else:
            stop = self.lookback + stop

        # Both start and stop are on left side.
        if start < 0 and stop < 0:
            fill_left_count = abs(start - stop)
            fill_right_count = 0
            start = stop = 0
        # Both start and stop are on right side.
        elif start >= len(self.data) and stop >= len(self.data):
            fill_right_count = abs(start - stop)
            fill_left_count = 0
            start = stop = len(self.data)
        # Set to 0 (beginning of actual episode) if result is a negative index.
        elif start < 0:
            fill_left_count = -start
            start = 0
        elif stop >= len(self.data):
            fill_right_count = stop - len(self.data)
            stop = len(self.data)

        assert start >= 0 and stop >= 0, (start, stop)
        assert start <= len(self.data) and stop <= len(self.data), (start, stop)
        slice_ = slice(start, stop, slice_.step)

        # Perform the actual slice.
        if self.finalized:
            data_slice = tree.map_structure(lambda s: s[slice_], self.data)
        else:
            data_slice = self.data[slice_]

        # Data is shorter than the range requested -> Fill the rest with `fill` data.
        if fill is not None and (fill_right_count > 0 or fill_left_count > 0):
            if self.finalized:
                if fill_left_count:
                    fill_batch = get_dummy_batch_for_space(
                        self.space,
                        fill_value=fill,
                        batch_size=fill_left_count,
                    )
                    data_slice = tree.map_structure(
                        lambda s0, s: np.concatenate([s0, s]), fill_batch, data_slice
                    )
                if fill_right_count:
                    fill_batch = get_dummy_batch_for_space(
                        self.space,
                        fill_value=fill,
                        batch_size=fill_right_count,
                    )
                    data_slice = tree.map_structure(
                        lambda s0, s: np.concatenate([s, s0]), fill_batch, data_slice
                    )

            else:
                fill_batch = [
                    get_dummy_batch_for_space(
                        self.space,
                        fill_value=fill,
                        batch_size=0,
                    )
                ]
                data_slice = (
                    fill_batch * fill_left_count
                    + data_slice
                    + fill_batch * fill_right_count
                )

        return data_slice

    def _get_int_index(
        self,
        idx: int,
        fill=None,
        neg_indices_left_of_zero=False,
    ):
        # If index >= 0 -> Ignore lookback buffer.
        # Otherwise, include lookback buffer.
        if idx >= 0 or neg_indices_left_of_zero:
            idx = self.lookback + idx

        try:
            if self.finalized:
                return tree.map_structure(lambda s: s[idx], self.data)
            else:
                return self.data[idx]
        # Out of range index -> If `fill`, use a fill dummy (B=0), if not, error out.
        except IndexError as e:
            if fill is not None:
                return get_dummy_batch_for_space(
                    self.space,
                    fill_value=fill,
                    batch_size=0,
                )
            else:
                raise e

    def _one_hot(self, data):
        if self.space is None:
            raise ValueError(
                f"Cannot `one_hot` data in `{type(self).__name__}` if a "
                "gym.Space was NOT provided during construction!"
            )

        def _convert(dat_, space):
            if isinstance(space, gym.spaces.Discrete):
                return one_hot(dat_, depth=space.n)
            elif isinstance(space, gym.spaces.MultiDiscrete):
                return one_hot_multidiscrete(dat_, depths=space.nvec)
            return dat_

        if self.finalized:
            data = tree.map_structure(_convert, data, self.space_struct)
        else:
            data = [
                tree.map_structure(_convert, dslice, self.space_struct)
                for dslice in data
            ]
        return data
