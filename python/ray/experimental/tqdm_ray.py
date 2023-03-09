import json
import os
import uuid
from typing import Any, Dict, Iterable, Optional

import colorama
import tqdm as real_tqdm

import ray
import ray._private.services as services

# Describes the state of a single progress bar.
ProgressBarState = Dict[str, Any]

# Magic token used to identify Ray TQDM log lines.
RAY_TQDM_MAGIC = "__ray_tqdm_magic_token__"


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
        self._desc = desc
        self._dump_state()

    def update(self, n=1):
        self._x += n
        self._dump_state()

    def close(self):
        self._closed = True
        self._dump_state()

    def _dump_state(self) -> None:
        print(json.dumps(self._get_state()))

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
    def __init__(self, state: ProgressBarState, pos_offset: int):
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
        if state["desc"] != self.state["desc"]:
            self.bar.set_description(state["desc"] + " " + str(state["pos"]))
        delta = state["x"] - self.state["x"]
        if delta:
            self.bar.update(delta)
        self.state = state

    def close(self):
        self.bar.close()

    def update_offset(self, pos_offset: int) -> None:
        if pos_offset != self.pos_offset:
            self.pos_offset = pos_offset
            self.bar.clear()
            self.bar.pos = -(pos_offset + self.state["pos"])
            self.bar.refresh()


class _Process:
    def __init__(self, ip, pid, pos_offset):
        self.ip = ip
        self.pid = pid
        self.pos_offset = pos_offset
        self.bars_by_uuid: Dict[str, _Bar] = {}

    def has_bar(self, bar_uuid) -> bool:
        return bar_uuid in self.bars_by_uuid

    def allocate_bar(self, state: ProgressBarState) -> None:
        self.bars_by_uuid[state["uuid"]] = _Bar(state, self.pos_offset)

    def update_bar(self, state: ProgressBarState) -> None:
        bar = self.bars_by_uuid[state["uuid"]]
        bar.update(state)

    def close_bar(self, state: ProgressBarState) -> None:
        bar = self.bars_by_uuid[state["uuid"]]
        bar.close()
        del self.bars_by_uuid[state["uuid"]]

    def max_pos(self):
        if not self.bars_by_uuid:
            return 0
        return max(bar.state["pos"] for bar in self.bars_by_uuid.values())

    def update_offset(self, offset: int) -> None:
        if offset != self.pos_offset:
            self.pos_offset = offset
            for bar in self.bars_by_uuid.values():
                bar.update_offset(offset)


class _BarManager:
    """Central tqdm manager run on the driver."""

    def __init__(self):
        self.ip = services.get_node_ip_address()
        self.processes = {}

    def process_state_update(self, state: ProgressBarState) -> None:
        if state["ip"] == self.ip:
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
        process = self.get_or_allocate_process(state)
        if process.has_bar(state["uuid"]):
            if state["closed"]:
                process.close_bar(state)
                self.update_offsets()
            else:
                process.update_bar(state)
        else:
            process.allocate_bar(state)
            self.update_offsets()

    def get_or_allocate_process(self, state: ProgressBarState):
        ptuple = (state["ip"], state["pid"])
        if ptuple not in self.processes:
            offset = sum(p.max_pos() + 1 for p in self.processes.values())
            self.processes[ptuple] = _Process(state["ip"], state["pid"], offset)
        return self.processes[ptuple]

    def update_offsets(self):
        offset = 0
        for proc in self.processes.values():
            proc.update_offset(offset)
            offset += proc.max_pos() + 1


_manager = _BarManager()


if __name__ == "__main__":
    import time

    bars = []

    @ray.remote
    def processing(delay):
        def sleep(x):
            time.sleep(delay)
            return x

        while True:
            ray.data.range(1000, parallelism=100).map(
                sleep, compute=ray.data.ActorPoolStrategy(1, 1)
            ).count()

    ray.get(
        [
            processing.remote(0.05),
            processing.remote(0.03),
            processing.remote(0.10),
        ]
    )
