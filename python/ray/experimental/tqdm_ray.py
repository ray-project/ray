import copy
import json
import os
import uuid
from typing import Any, Dict, Iterable, Optional

import colorama
import tqdm as real_tqdm

import ray

# Describes the state of a single progress bar.
ProgressBarState = Dict[str, Any]

# Magic token used to identify Ray TQDM log lines.
RAY_TQDM_MAGIC = "__ray_tqdm_magic_token__"

# Global manager singleton.
_manager: Optional["_BarManager"] = None


class tqdm:
    """Experimental: Ray distributed tqdm implementation.

    This class lets you use tqdm from any Ray remote task or actor, and have the
    progress centrally reported from the driver. This avoids issues with overlapping
    / conflicting progress bars, as the driver centrally manages tqdm positions.

    Supports a limited subset of tqdm args.
    """

    def __init__(
        self,
        iterable: Optional[Iterable] = None,
        desc: Optional[str] = None,
        total: Optional[int] = None,
        position: Optional[int] = 0,
        # Visible for testing.
        _ray_ip: Optional[str] = None,
        _ray_pid: Optional[int] = None,
    ):
        import ray._private.services as services

        if iterable is not None:
            raise NotImplementedError("TODO implement iterable support")
        if position is None:
            raise NotImplementedError("tqdm_ray requires position to be specified")
        self._iterable = iterable
        self._desc = desc
        self._total = total
        self._position = position
        self._ip = _ray_ip or services.get_node_ip_address()
        self._pid = _ray_pid or os.getpid()
        self._pos = position
        self._uuid = uuid.uuid4().hex
        self._x = 0
        self._closed = False

    def set_description(self, desc):
        """Implements tqdm.tqdm.set_description."""
        self._desc = desc
        self._dump_state()

    def update(self, n=1):
        """Implements tqdm.tqdm.update."""
        self._x += n
        self._dump_state()

    def close(self):
        """Implements tqdm.tqdm.close."""
        self._closed = True
        self._dump_state()

    def _dump_state(self) -> None:
        if ray._private.worker.global_worker.mode == ray.WORKER_MODE:
            # Include newline in payload to avoid split prints.
            # TODO(ekl) we should move this to events.json to avoid log corruption.
            print(json.dumps(self._get_state()) + "\n", end="")
        else:
            instance().process_state_update(copy.deepcopy(self._get_state()))

    def _get_state(self) -> ProgressBarState:
        return {
            "__magic_token__": RAY_TQDM_MAGIC,
            "x": self._x,
            "pos": self._pos,
            "desc": self._desc,
            "total": self._total,
            "ip": self._ip,
            "pid": self._pid,
            "uuid": self._uuid,
            "closed": self._closed,
        }


class _Bar:
    """Manages a single virtual progress bar on the driver.

    The actual position of individual bars is calculated as (pos_offset + position),
    where `pos_offset` is the position offset determined by the BarManager.
    """

    def __init__(self, state: ProgressBarState, pos_offset: int):
        """Initialize a bar.

        Args:
            state: The initial progress bar state.
            pos_offset: The position offset determined by the BarManager.
        """
        self.state = state
        self.pos_offset = pos_offset
        self.bar = real_tqdm.tqdm(
            desc=state["desc"] + " " + str(state["pos"]),
            total=state["total"],
            position=pos_offset + state["pos"],
            leave=False,
        )
        if state["x"]:
            self.bar.update(state["x"])

    def update(self, state: ProgressBarState) -> None:
        """Apply the updated worker progress bar state."""
        if state["desc"] != self.state["desc"]:
            self.bar.set_description(state["desc"] + " " + str(state["pos"]))
        delta = state["x"] - self.state["x"]
        if delta:
            self.bar.update(delta)
        self.state = state

    def close(self):
        """The progress bar has been closed."""
        self.bar.close()

    def update_offset(self, pos_offset: int) -> None:
        """Update the position offset assigned by the BarManager."""
        if pos_offset != self.pos_offset:
            self.pos_offset = pos_offset
            self.bar.clear()
            self.bar.pos = -(pos_offset + self.state["pos"])
            self.bar.refresh()


class _BarGroup:
    """Manages a group of virtual progress bar produced by a single worker.

    All the progress bars in the group have the same `pos_offset` determined by the
    BarManager for the process.
    """

    def __init__(self, ip, pid, pos_offset):
        self.ip = ip
        self.pid = pid
        self.pos_offset = pos_offset
        self.bars_by_uuid: Dict[str, _Bar] = {}

    def has_bar(self, bar_uuid) -> bool:
        """Return whether this bar exists."""
        return bar_uuid in self.bars_by_uuid

    def allocate_bar(self, state: ProgressBarState) -> None:
        """Add a new bar to this group."""
        self.bars_by_uuid[state["uuid"]] = _Bar(state, self.pos_offset)

    def update_bar(self, state: ProgressBarState) -> None:
        """Update the state of a managed bar in this group."""
        bar = self.bars_by_uuid[state["uuid"]]
        bar.update(state)

    def close_bar(self, state: ProgressBarState) -> None:
        """Remove a bar from this group."""
        bar = self.bars_by_uuid[state["uuid"]]
        bar.close()
        del self.bars_by_uuid[state["uuid"]]

    def slots_required(self):
        """Return the number of pos slots we need to accomodate bars in this group."""
        if not self.bars_by_uuid:
            return 0
        return 1 + max(bar.state["pos"] for bar in self.bars_by_uuid.values())

    def update_offset(self, offset: int) -> None:
        """Update the position offset assigned by the BarManager."""
        if offset != self.pos_offset:
            self.pos_offset = offset
            for bar in self.bars_by_uuid.values():
                bar.update_offset(offset)

    def hide_bars(self) -> None:
        """Temporarily hide visible bars to avoid conflict with other log messages."""
        for bar in self.bars_by_uuid.values():
            bar.bar.clear()

    def unhide_bars(self) -> None:
        """Opposite of hide_bars()."""
        for bar in self.bars_by_uuid.values():
            bar.bar.refresh()


class _BarManager:
    """Central tqdm manager run on the driver.

    This class holds a collection of BarGroups and updates their `pos_offset` as
    needed to ensure individual progress bars do not collide in position, kind of
    like a virtual memory manager.
    """

    def __init__(self):
        import ray._private.services as services

        self.ip = services.get_node_ip_address()
        self.pid = os.getpid()
        self.bar_groups = {}
        self.in_hidden_state = False
        self.num_hides = 0

    def process_state_update(self, state: ProgressBarState) -> None:
        """Apply the remote progress bar state update.

        This creates a new bar locally if it doesn't already exist. When a bar is
        created or destroyed, we also recalculate and update the `pos_offset` of each
        BarGroup on the screen.
        """
        if self.in_hidden_state:
            self.unhide_bars()
        if state["ip"] == self.ip:
            if state["pid"] == self.pid:
                prefix = ""
            else:
                prefix = "{}{}(pid={}){} ".format(
                    colorama.Style.DIM,
                    colorama.Fore.CYAN,
                    state.get("pid"),
                    colorama.Style.RESET_ALL,
                )
        else:
            prefix = "{}{}(pid={}, ip={}){} ".format(
                colorama.Style.DIM,
                colorama.Fore.CYAN,
                state.get("pid"),
                state.get("ip"),
                colorama.Style.RESET_ALL,
            )
        state["desc"] = prefix + state["desc"]
        process = self._get_or_allocate_bar_group(state)
        if process.has_bar(state["uuid"]):
            if state["closed"]:
                process.close_bar(state)
                self._update_offsets()
            else:
                process.update_bar(state)
        else:
            process.allocate_bar(state)
            self._update_offsets()

    def hide_bars(self) -> None:
        """Temporarily hide visible bars to avoid conflict with other log messages."""
        if not self.in_hidden_state:
            self.in_hidden_state = True
            self.num_hides += 1
            for group in self.bar_groups.values():
                group.hide_bars()

    def unhide_bars(self) -> None:
        """Opposite of hide_bars()."""
        if self.in_hidden_state:
            self.in_hidden_state = False
            for group in self.bar_groups.values():
                group.unhide_bars()

    def _get_or_allocate_bar_group(self, state: ProgressBarState):
        ptuple = (state["ip"], state["pid"])
        if ptuple not in self.bar_groups:
            offset = sum(p.slots_required() for p in self.bar_groups.values())
            self.bar_groups[ptuple] = _BarGroup(state["ip"], state["pid"], offset)
        return self.bar_groups[ptuple]

    def _update_offsets(self):
        offset = 0
        for proc in self.bar_groups.values():
            proc.update_offset(offset)
            offset += proc.slots_required()


def instance() -> _BarManager:
    """Get or create a BarManager for this process."""
    global _manager
    if _manager is None:
        _manager = _BarManager()
    return _manager


if __name__ == "__main__":
    import time

    @ray.remote
    def processing(delay):
        def sleep(x):
            print("Intermediate result", x)
            time.sleep(delay)
            return x

        ray.data.range(1000, parallelism=100).map(
            sleep, compute=ray.data.ActorPoolStrategy(1, 1)
        ).count()

    ray.get(
        [
            processing.remote(0.03),
            processing.remote(0.01),
            processing.remote(0.05),
        ]
    )
