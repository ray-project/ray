from typing import List, Optional, Union


class TimestepMappingWithInfiniteLookback:
    def __init__(self, timesteps: list = None, lookback: int = 0, t_started: int = 0):
        """Initializes an instance of the class."""
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
        return len(self.timesteps) - self.lookback

    def __getitem__(self, item):
        """Gets a global timestep."""
        return self.timesteps[item]

    def __add__(self, other):
        if self.timesteps[-1] > other[other.lookback]:
            raise RuntimeError("Cannot add instances with overlapping timesteps.")
        else:
            # TODO (simon): This can still add mappings of chunks where
            # another one is in between (even when using ids)
            return TimestepMappingWithInfiniteLookback(
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
        return TimestepMappingWithInfiniteLookback(
            timesteps=timesteps,
            lookback=lookback,
            t_started=t_started,
        )

    def get_local_timesteps(
        self,
        global_timesteps: Optional[Union[int, List[int], slice]] = None,
        neg_timesteps_left_of_zero: bool = False,
        fill: float = None,
        t: int = 0,
    ) -> List[int]:

        if not self.timesteps:
            return None if isinstance(global_timesteps, int) else []

        # If no global timesteps are provided return all of them.
        if global_timesteps is None:
            return self._get_all_local_timesteps()
        elif isinstance(global_timesteps, slice):
            return self._get_local_timestep_slice(
                global_timesteps,
                neg_timesteps_left_of_zero=neg_timesteps_left_of_zero,
                t=t,
            )
        elif isinstance(global_timesteps, list):
            return self._get_local_timesteps_slice(
                global_timesteps,
                neg_timesteps_left_of_zero=neg_timesteps_left_of_zero,
                t=t,
            )
        # Must be single time step.
        else:
            assert isinstance(global_timesteps, int)
            return self._get_single_local_timestep(
                global_timesteps,
                neg_timesteps_left_of_zero=neg_timesteps_left_of_zero,
                t=t,
            )

    def _get_all_local_timesteps(self):
        # TODO (simon): Check, if we have to return everything after
        # t_started - lookback (e.g. initial one in SAE is not anymore in
        # global lookback: t_started=100, lookback=10, self.lookback=0,
        # self.timesteps[self.lookback=0]=88).
        return self.timesteps

    def _get_single_local_timestep(
        self, global_timesteps, neg_timesteps_left_of_zero, t, shift=0
    ):
        # User wants negative timesteps.
        if global_timesteps < 0:
            # User wants to index into the lookback before `t_started`.
            if neg_timesteps_left_of_zero:
                # Translate negative timestep to global timestep.
                global_timesteps = global_timesteps + self.t_started
                shift -= self.lookback
            # User wants to lookback from actual global timesteps `t`.
            else:
                # Translate negative timestep to global timestep
                global_timesteps = t + global_timesteps

        # Only return if the global timestep is present in `timesteps`.
        return (
            self.timesteps.index(global_timesteps) + shift
            if global_timesteps in self.timesteps
            else None
        )

    def _get_local_timestep_slice(
        self,
        global_timesteps,
        neg_timesteps_left_of_zero,
        t,
        shift=0,
    ):
        # Time steps are given by slice.
        start = global_timesteps.start
        stop = global_timesteps.stop
        step = global_timesteps.step

        if start is None:
            # Exclude the lookback buffer.
            # TODO (simon): Maybe change to `t_started` for the case
            # `self.timesteps=[]`.
            start = self.timesteps[self.lookback]
        # User wants negative timesteps.
        elif start < 0:
            # User wants to index into lookback buffer.
            if neg_timesteps_left_of_zero:
                # Translate negative timestep to global timestep.
                start += self.t_started
                shift -= self.lookback
            # User wants to lookback from actual global timesteps `t`.
            else:
                # Translate negative timestep to global timestep
                start += t
                start = min(self.timesteps[0], start)
                shift -= self.lookback

        if stop is None:
            # Search for timesteps until the end.
            stop = self.timesteps[-1] + 1
        # Stop is negative.
        elif stop < 0:
            # User wants to index into lookback buffer.
            if neg_timesteps_left_of_zero:
                # Translate negative timestep to global timestep.
                stop += self.t_started
                if start >= 0:
                    shift -= self.lookback
            # User wants to lookback from actual global timesteps `t`.
            else:
                # Translate negative timestep to global timestep
                stop += t

        global_timesteps = (
            list(range(start, stop, step)) if step else list(range(start, stop))
        )

        return self._get_local_timestep_list(
            global_timesteps,
            neg_timesteps_left_of_zero=neg_timesteps_left_of_zero,
            t=t,
            shift=shift,
        )

    def _get_local_timestep_list(
        self, global_timesteps, neg_timesteps_left_of_zero, t, shift=0
    ):
        local_timesteps = [
            self._get_single_local_timestep(
                ts, neg_timesteps_left_of_zero, t, shift=shift
            )
            for ts in global_timesteps
        ]
        return [ts for ts in local_timesteps if ts is not None]
