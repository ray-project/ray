"""Reference implementation of the progress ledger on **unmodified** Ray.

Everything the stage-A model checker proved is here, and nothing it refuted:

* M1   epoch fencing: absent-key CAS, then **read back**, then epoch-scoped keys
* M1b  carry-forward reads segments FIRST and ``base_ptr`` LAST (C17)
* M1c  carry-forward scans epochs **ascending** (C20)
* M2   commit coalescing on a window ``W``
* M3   compaction: publish base, flip pointer, then delete -- in that order (C6)
* per-**incarnation** instance ids, never actor-derived (C13)
* ledger keys scoped by ``session_name`` (C12)

The one thing that is easy to get wrong and is called out in code: on Ray 2.58
``_internal_kv_put`` returns **True when the key ALREADY EXISTED**
(``ray/experimental/internal_kv.py``, docstring: *"Returns: Whether the value
already exists"*). It is the inverse of what "did my conditional put succeed?"
reads like, so this file never relies on it -- M1 reads the value back instead.
"""

from __future__ import annotations

import json
import time
import uuid
from typing import Dict, List, Optional, Tuple

import ray
from ray.experimental import internal_kv as ikv

NS = b"progress_ledger"
CARRY_FORWARD_RETRIES = 4


def _b(s: str) -> bytes:
    return s.encode()


class Ledger:
    """The durable progress ledger, over GCS internal_kv."""

    def __init__(self, session: str, job: str):
        self.root = f"{session}/{job}"
        self.epoch: Optional[int] = None
        self.instance_id = uuid.uuid4().hex  # PER-INCARNATION. Never the actor id.
        self.writes = 0

    # -- keys -------------------------------------------------------------
    def _ep(self) -> str:
        return f"{self.root}/epoch/"

    def _ekey(self, e: int) -> str:
        return f"{self._ep()}{e:06d}"

    def _sp(self, e: int) -> str:
        return f"{self.root}/state/{e:06d}/"

    def _base(self, e: int, n: int) -> str:
        return f"{self._sp(e)}base@{n:06d}"

    def _ptr(self, e: int) -> str:
        return f"{self._sp(e)}base_ptr"

    def _seg(self, e: int, i: int) -> str:
        return f"{self._sp(e)}seg/{i:06d}"

    # -- primitives -------------------------------------------------------
    def _put(self, key: str, value, overwrite: bool = True):
        self.writes += 1
        return ikv._internal_kv_put(_b(key), _b(json.dumps(value)), overwrite,
                                    namespace=NS)

    def _get(self, key: str):
        raw = ikv._internal_kv_get(_b(key), namespace=NS)
        return None if raw is None else json.loads(raw)

    def _del(self, key: str):
        self.writes += 1
        return ikv._internal_kv_del(_b(key), namespace=NS)

    def _list(self, prefix: str) -> List[str]:
        got = ikv._internal_kv_list(_b(prefix), namespace=NS) or []
        return sorted(k.decode() if isinstance(k, bytes) else k for k in got)

    # -- M1: claim an epoch ----------------------------------------------
    def claim(self) -> bool:
        epochs = sorted(int(k.rsplit("/", 1)[1]) for k in self._list(self._ep()))
        e = (max(epochs) + 1) if epochs else 1
        # NOTE: the return value of this put is deliberately ignored. On 2.58 it
        # is True when the key ALREADY EXISTED, and an infinitely-retried put
        # cannot tell a lost ack from a lost race anyway.
        self._put(self._ekey(e), self.instance_id, overwrite=False)
        if self._get(self._ekey(e)) != self.instance_id:
            return False
        self.epoch = e
        self._carry_forward(epochs)
        return True

    # -- M1b + M1c: the read orders that two refutations paid for ---------
    def _read_epoch(self, e: int) -> Optional[dict]:
        segs: Dict[int, dict] = {}
        for k in self._list(self._sp(e) + "seg/"):
            v = self._get(k)
            if v is not None:
                segs[int(k.rsplit("/", 1)[1])] = v
        base = ptr = None
        for _ in range(CARRY_FORWARD_RETRIES):
            ptr = self._get(self._ptr(e))
            if ptr is None:
                return None
            base = self._get(self._base(e, ptr))
            if base is not None:
                break
        if base is None:
            return None
        done = set(base["done"])
        seq = base["seq"]
        for i, v in segs.items():
            if i > ptr:
                done |= set(v["done"])
                seq = max(seq, i)
        return {"seq": seq, "done": sorted(done)}

    def _carry_forward(self, epochs: List[int]) -> None:
        state = {"seq": 0, "done": []}
        for prev in sorted(epochs):  # ASCENDING -- C20
            got = self._read_epoch(prev)
            if got is not None:
                state = got
        self.state = state
        self._put(self._base(self.epoch, state["seq"]), state)
        self._put(self._ptr(self.epoch), state["seq"])
        self._sweep()

    def _sweep(self) -> None:
        for k in self._list(f"{self.root}/state/"):
            try:
                owner = int(k[len(f"{self.root}/state/"):].split("/", 1)[0])
            except ValueError:
                continue
            if owner < self.epoch:
                self._del(k)
        for k in self._list(self._ep()):
            if int(k.rsplit("/", 1)[1]) < self.epoch:
                self._del(k)

    # -- M2 / M3 ----------------------------------------------------------
    def commit(self, seq: int, batch: List[int]) -> None:
        self._put(self._seg(self.epoch, seq), {"seq": seq, "done": sorted(batch)})

    def compact(self, seq: int, done) -> None:
        self._put(self._base(self.epoch, seq), {"seq": seq, "done": sorted(done)})
        self._put(self._ptr(self.epoch), seq)  # linearization point
        for k in self._list(self._sp(self.epoch)):
            tail = k.rsplit("/", 1)[1]
            if k.endswith("base_ptr"):
                continue
            if "/seg/" in k and int(tail) <= seq:
                self._del(k)
            elif tail.startswith("base@") and int(tail[5:]) < seq:
                self._del(k)

    def key_count(self) -> int:
        return len(self._list(self.root))


@ray.remote(max_restarts=-1, max_task_retries=-1)
class JobCoordinator:
    """Detached, named, restartable.  Owns the work; the driver owns nothing."""

    def __init__(self, session: str, job: str, n_units: int, w: int, k: int,
                 unit_seconds: float = 0.02, queue_depth: int = 0):
        self.cfg = (n_units, w, k, unit_seconds)
        self.ledger = Ledger(session, job)
        self.executed: List[int] = []
        self.finished = False
        self.fatal: Optional[str] = None
        # C26: queue_depth > 0 moves commits OFF the unit loop's critical path,
        # behind a bounded FIFO drained by a single writer thread.  Single
        # writer, FIFO, so commit order is preserved by construction; bounded,
        # so the redo window is Q + W rather than unbounded.
        self.queue_depth = queue_depth
        self.durable_units = 0
        self.writer_error: Optional[str] = None
        self._q = None
        # Recovery runs in __init__, not on first request -- ray#65037 is the
        # cautionary tale for doing it lazily.
        if not self.ledger.claim():
            self.fatal = "lost the epoch race; a newer incarnation owns this job"
        self.durable_units = len(self.ledger.state.get("done", [])) \
            if getattr(self.ledger, "state", None) else 0
        self._resumed_from = self.durable_units
        if queue_depth > 0 and not self.fatal:
            import queue as _queue
            import threading as _threading
            self._q = _queue.Queue(maxsize=queue_depth)
            self._writer_thread = _threading.Thread(
                target=self._writer_loop, daemon=True)
            self._writer_thread.start()

    def _writer_loop(self) -> None:
        """Single writer.  Every KV mutation after startup happens here."""
        n, w, k, _ = self.cfg
        since_compact = 0
        done = set(self.ledger.state["done"])
        while True:
            item = self._q.get()
            if item is None:
                self._q.task_done()
                return
            seq, batch = item
            try:
                self.ledger.commit(seq, batch)
                done |= set(batch)
                self.durable_units = len(done)
                since_compact += 1
                if since_compact >= k:
                    self.ledger.compact(seq, done)
                    since_compact = 0
            except Exception as e:  # recorded, never swallowed
                self.writer_error = "%s: %s" % (type(e).__name__, str(e)[:200])
            finally:
                self._q.task_done()

    def die(self, delay: float = 0.5, death_path: str = "") -> None:
        """Hard-kill this incarnation shortly, in-place.

        Needed to measure C26's redo window: the backlog only exists WHILE GCS
        is down, and ray.kill goes through the GCS actor manager, which is
        exactly what is unavailable then.  A cached actor handle can still reach
        the worker directly, so the sampler calls this.

        It RETURNS before exiting, on purpose.  An earlier version called
        os._exit inline, so the task was in flight when the actor died, and
        max_task_retries=-1 replayed it on every restart -- an infinite
        suicide loop that voided runs/20260910-...-C28storm and produced the
        bogus 18-incarnation reading that raised C28 in the first place.
        """
        import threading

        def _go():
            # Dump state at the INSTANT of death, to a local file.  Sampling
            # every 2s blurs the count by several units, which is fatal for
            # C27's exact-formula claim; and this path must work with GCS down.
            if death_path:
                try:
                    with open(death_path, "w") as f:
                        json.dump(self.progress(), f)
                except Exception:
                    pass
            __import__("os")._exit(1)

        threading.Timer(delay, _go).start()

    def progress(self) -> dict:
        """In-memory only.  Touches NO key-value store.

        `info()` calls key_count(), which is a KV list, so during a GCS outage
        info() blocks and cannot distinguish "wedged" from "working".  With
        max_concurrency>1 this method still answers from another thread, which
        makes "work continues, durability pauses" observable at all.
        """
        return {
            "epoch": self.ledger.epoch,
            "executed_this_incarnation": len(self.executed),
            "committed_this_incarnation": self.ledger.writes,
            "durable_units": self.durable_units,
            "queued": self._q.qsize() if self._q is not None else 0,
            "queue_depth": self.queue_depth,
            "resumed_from": getattr(self, "_resumed_from", 0),
            "writer_error": self.writer_error,
            "finished": self.finished,
            "fatal": self.fatal,
            "pid": __import__("os").getpid(),
        }

    def info(self) -> dict:
        return {
            "epoch": self.ledger.epoch,
            "instance_id": self.ledger.instance_id,
            "resumed_from": len(getattr(self.ledger, "state", {}).get("done", [])),
            "executed_this_incarnation": len(self.executed),
            "finished": self.finished,
            "fatal": self.fatal,
            "ledger_writes": self.ledger.writes,
            "ledger_keys": self.ledger.key_count(),
            "pid": __import__("os").getpid(),
        }

    def run(self) -> dict:
        if self.fatal:
            return self.info()
        n, w, k, us = self.cfg
        done = set(self.ledger.state["done"])
        seq = self.ledger.state["seq"]
        pending: List[int] = []
        since_compact = 0
        for u in [u for u in range(n) if u not in done]:
            time.sleep(us)  # the unit's real work
            self.executed.append(u)
            pending.append(u)
            if len(pending) < w:
                continue
            seq += 1
            if self._q is not None:
                # Blocks when full: backpressure, so the redo window stays
                # bounded by Q rather than growing without limit.
                self._q.put((seq, list(pending)))
                done |= set(pending)
                pending = []
                continue
            self.ledger.commit(seq, pending)
            self.durable_units = len(done | set(pending))
            done |= set(pending)
            pending = []
            since_compact += 1
            if since_compact >= k:
                self.ledger.compact(seq, done)
                since_compact = 0
        if pending:
            seq += 1
            if self._q is not None:
                self._q.put((seq, list(pending)))
            else:
                self.ledger.commit(seq, pending)
            done |= set(pending)
        if self._q is not None:
            self._q.join()
        self.finished = True
        return self.info()
