from typing import List, Optional, Union


class InfiniteLookbackTimestepMapping:
    """Stores and manages mappings from global to local agent timesteps.

    This class stores a list of global timesteps, i.e. environment steps in a
    multi-agent setup. Each agent in such a multi-agent setup is provided with
    an instance of this class. The list indices are the local timesteps of the
    agent, including the initial observation (i.e. local timestep (agent step)
    = 0). Each time an agent steps in the multi-agent environment another global
    timestep is added to this class.

    The lookback allows a view into global timesteps in another multi-agent episode
    chunk. This is useful for cases in which episode data had been collected and
    returned before an episode was done and in a second rollout this episode is
    continued. The lookback enables a view into past global timesteps at which the
    corresponding agent had stepped. This is important when requesting data from a
    multi-agent episode with past experiences, e.g. if a stateful model is used.

    See also `SingleAgentEpisode`.
    """

    def __init__(self, timesteps: list = None, lookback: int = 0, t_started: int = 0):
        """Initializes an instance of the class.

        Args:
            timesteps: Optional. A `list` of time steps from collected multi-agent
                observations.
            lookback: Optional. The required length for the lookback buffer. The
                lookback is considered a global one, i.e. for the multi-agent
                environment/episode that defines the time steps. However, not each
                agent might have stepped at each time step requested in this lookback.
                As a result the lookback in each time step mapping (i.e.
                `self.lookback`) might differ from the requested lookback here.
            t_started: The global time step at which the global lookback should start
                from. As explained in the description of the `lookback` argument,
                not each agent might have stepped at each time step during the lookback
                and therefore we need the starting point as a reference point.
        """
        self.timesteps = timesteps or []

        # The `lookback` must be consistent with `timesteps` and `t_started`
        self.lookback = lookback
        self.t_started = t_started

    def __repr__(self) -> str:
        """Represents this class.

        Prints out the list of global timesteps.
        """
        return self.timesteps.__repr__()

    def __len__(self) -> int:
        """The length of this `_TimestepMapping`.

        This is the number of global timesteps the agent stepped without
        the lookback.
        """
        return len(self.timesteps[self.lookback :])  # - self.lookback

    def __iter__(self):
        return iter(self.timesteps)

    def __getitem__(self, item):
        """Gets a global timestep."""
        return self.timesteps[item]

    def __add__(self, other):
        if self.timesteps[-1] > other[other.lookback]:
            raise RuntimeError("Cannot add instances with overlapping timesteps.")
        else:
            # TODO (simon): This can still add mappings of chunks where
            # another one is in between (even when using ids)
            return InfiniteLookbackTimestepMapping(
                timesteps=self.timesteps + other.timesteps[other.lookback :],
                lookback=self.lookback,
                t_started=self.t_started,
            )

    def __iadd__(self, other):
        if self.timesteps[-1] > other[other.lookback]:
            raise RuntimeError("Cannot add instances with overlapping timesteps.")
        else:
            # TODO (simon): This can still add mappings of chunks where
            # another one is in between (even when using ids)
            self.timesteps += other.timesteps[other.lookback :]
            return self

    def append(self, item: int) -> None:
        """Appends global timesteps."""
        if len(self) > 0 and item <= self.timesteps[-1]:
            raise RuntimeError(
                "Cannot `append` a timestep prior to the last one: "
                f"{self.timesteps[-1]}"
            )
        self.timesteps.append(item)

    def extend(self, items: List[int]) -> None:
        """Extends the global timesteps."""
        # Make sure all new timesteps come in ascending order.
        items = sorted(items)

        # Assert, timesteps are not overlapping.
        if len(self) > 0 and items[0] <= self.timesteps[-1]:
            raise RuntimeError(
                "Cannot `append` a timestep prior to the last one: "
                f"{self.timesteps[-1]}"
            )
        # If all fine, extend.
        self.timesteps.extend(items)

    def cut(self, lookback, t_started):
        # We use the lookback plus the initial observation (+1).
        indices = self.get_local_timesteps(slice(t_started - lookback, t_started + 1))

        if indices:
            timesteps = [self.timesteps[idx] for idx in indices]
            lookback = len(timesteps) - 1
        else:
            # Agent has not seen his initial observation, yet.
            timesteps = []
            lookback = 0

        # TODO (simon): What to do with the initial local timestep?
        # `t_started could be way after that one for an agent.
        # Maybe we then just keep the last and make the lookback to zero.
        # Then it will never been found when `global_ts` = True.
        # The only timesteps that count here (and the lookback and initial ones
        # are the global ones not the local ones with global_ts=True)
        return InfiniteLookbackTimestepMapping(
            timesteps=timesteps,
            lookback=lookback,
            t_started=t_started,
        )

    def get_local_timesteps(
        self,
        global_timesteps: Optional[Union[int, List[int], slice]] = None,
        *,
        neg_timesteps_left_of_zero: bool = False,
        #fill: float = None,
        #return_none: bool = False,
        t: int = 0,
        #shift: int = 0,
    ) -> List[int]:
        """Maps global timesteps to corresponding local ones.

        Note, if the internal starting timestep `self.t_started` equals `t`
        the results for `neg_timesteps_left_of_zero` are identical to the ones
        with `neg_timesteps_left_of_zero=False`.

        """

        if not self.timesteps:
            return None if isinstance(global_timesteps, int) else []

        # If no global timesteps are provided return all of them.
        if global_timesteps is None:
            return self._get_all_local_timesteps()
        # If a `slice` is requested, return a `slice` as long as `return_none=False`.
        elif isinstance(global_timesteps, slice):
            return self._get_local_timestep_slice(
                global_timesteps,
                neg_timesteps_left_of_zero=neg_timesteps_left_of_zero,
                t=t,
                #shift=shift,
                #return_none=return_none,
            )
        # If timesteps come as a `list`, we also return a `list`
        elif isinstance(global_timesteps, list):
            return self._get_local_timestep_list(
                global_timesteps,
                neg_timesteps_left_of_zero=neg_timesteps_left_of_zero,
                t=t,
                #shift=shift,
                #return_none=return_none,
            )
        # Must be single time step.
        else:
            assert isinstance(global_timesteps, int)
            return self._get_single_local_timestep(
                global_timesteps,
                neg_timesteps_left_of_zero=neg_timesteps_left_of_zero,
                #return_none=return_none,
                t=t,
                #shift=shift,
            )

    def _get_all_local_timesteps(self):
        # TODO (simon): Check, if we have to return everything after
        # t_started - lookback (e.g. initial one in SAE is not anymore in
        # global lookback: t_started=100, lookback=10, self.lookback=0,
        # self.timesteps[self.lookback=0]=88).
        return self.timesteps

    def _get_single_local_timestep(
        self, global_timesteps, neg_timesteps_left_of_zero, t, #return_none, t #, shift=0
    ):
        # User wants negative timesteps.
        if global_timesteps < 0:
            # User wants to index into the lookback before `t_started`.
            if neg_timesteps_left_of_zero:
                # Translate negative timestep to global timestep.
                global_timesteps = global_timesteps + self.t_started
            # User wants to lookback from actual global timesteps `t`.
            #else:
            #    # Translate negative timestep to global timestep
            #    global_timesteps = t + global_timesteps

        # Only return if the global timestep is present in `timesteps`.
        # Shift back or forth by `shift`, e.g. for searching for actions.
        #if (global_timesteps + shift) in self.timesteps:
        if global_timesteps in self.timesteps:
            return self.timesteps.index(global_timesteps)
            #return [self.timesteps.index(global_timesteps + shift) - self.lookback]
        #elif return_none:
        #    return None
        else:
            None

    def _get_local_timestep_slice(
        self,
        global_timesteps: slice,
        neg_timesteps_left_of_zero: bool,
        return_none: bool,
        t: int,
        #shift: int = 0,
    ):
        # Retrieve the slice's attributes.
        start = global_timesteps.start
        stop = global_timesteps.stop
        step = global_timesteps.step

        if (start is not None and stop is not None and step is not None) and stop < start and step > 0:
            return []

        if start is None:
            # Exclude the lookback buffer.
            start = self.timesteps[self.lookback]
        elif start < 0:
            # User wants to index into lookback buffer.
            if neg_timesteps_left_of_zero:
                # Translate relative timestep to total one.
                start += self.t_started
            # Otherwise, (the default), the user wants to look back from 't'.
            else:
                # Translate relative timestep (towards `t`) to total one.
                start += t + 1

        if stop is None:
            # User wants to have a ascending order in the slice.
            if step is None or step > 0:
                # Stop after the last recorded global timestep.
                stop = self.timesteps[-1] + 1
            # User wants a slice in descending order.
            else:
                # Stop downwards before the lookback.
                stop = self.timesteps[self.lookback]
        elif stop < 0:
            # User wants to index into lookback buffer.
            if neg_timesteps_left_of_zero:
                # Translate relative timestep (towards 'self.t_started') to total one.
                stop += self.t_started
            # Otherwise, (the default), the user wants to look back from 't'.
            else:
                # Translate relative timestep (towards `t`) to total one.
                stop += t + 1

        # Generate a list from the slice.
        global_timesteps = (
            list(range(start, stop, step)) if step else list(range(start, stop))
        )

        # Retrieve all indices in the list. Note, we do not know which indices
        # are contained in `self.timesteps` so requesting `start` and `stop` is
        # not enough.
        # TODO (simon): Write a _get_left_from and _get_right_from to improve
        # performance.
        local_timesteps = self._get_local_timestep_list(
            global_timesteps,
            neg_timesteps_left_of_zero=neg_timesteps_left_of_zero,
            return_none=return_none,
            t=t,
            shift=shift,
        )

        # Generate a slice from retrieved local timesteps.
        if local_timesteps and not return_none:
            return slice(local_timesteps[0], local_timesteps[-1] + 1, step)
        # If we need to return `None` for non-existent timesteps we return a `list`.
        else:
            return local_timesteps

    def _get_local_timestep_list(
        self, global_timesteps, neg_timesteps_left_of_zero, return_none, t, shift=0
    ):
        # Get all local timesteps.
        local_timesteps = [
            self._get_single_local_timestep(
                ts, neg_timesteps_left_of_zero, return_none, t, shift=shift
            )
            for ts in global_timesteps
        ]

        # Non-existent timesteps should be returned as `None`.
        if return_none:
            return [ts[0] if ts else ts for ts in local_timesteps]
        # Return only local timesteps for global ones that exist in `self.timesteps`.
        else:
            return [ts[0] for ts in local_timesteps if ts]
