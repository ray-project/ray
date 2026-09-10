"""A model of Ray's GCS ``internal_kv``, faithful to the properties that matter.

Everything modelled here was verified against ``ray-project/ray@80142ba`` (master,
2026-09-09).  The point of the fake is not to be a key-value store -- it is to be
*exactly as weak* as ``internal_kv`` is, so that a protocol which passes here has
not accidentally relied on a guarantee Ray does not offer.

What is modelled, and why:

1. **One flat shared table.**  Namespaces are a string prefix on the key, not a
   separate table (``store_client_kv.cc:29-37,52-54``); every ``internal_kv``
   user in the cluster lands in ``TablePrefix::KV``.  So a prefix scan costs
   O(size of the whole table), and that cost is counted here -- it is the reason
   a key-per-work-unit ledger was refuted on design grounds.

2. **``put(overwrite=False)`` is the only atomic conditional.**  There is no
   compare-and-swap on values, no multi-key atomicity and no transaction.

3. **The return value of a put is inverted in the Python wrapper.**  The raw
   client returns the number of keys *added* (``gcs_client.pxi:125-139``), and
   ``_internal_kv_put`` returns ``raw == 0`` (``internal_kv.py:100``) -- i.e.
   **True when the key already existed**, which is the opposite of what "did my
   conditional put succeed?" reads like.  Both forms are exposed so a protocol
   can get this wrong and be caught doing it.

4. **An acked mutation is on disk.**  ``WriteOptions::sync = true``
   unconditionally (``rocksdb_store_client.cc:104-117``) and the RPC reply is
   sent from inside the post-fsync callback.  Consequently this fake has *no*
   lost-write fault: a mutation either did not happen or is durable.  What it
   does have is lost *acks*, which is a different and much more interesting
   failure -- see ``checker.py``.

5. **Every mutation costs one fsync (~3.81 ms measured in REP-64).**  Counted,
   never slept, so the ordering experiments in stage A hand the cost model to
   stage B for free.

Deliberately not modelled: RocksDB itself, gRPC, latency, and the GCS actor
registry.  Each is a registered fidelity gap in ``claims.json``.
"""

from __future__ import annotations

import copy
from typing import Any, Dict, List, Optional

#: REP-64 benchmark, ext4, p50.  Used only to convert fsync counts into seconds.
FSYNC_SECONDS = 0.00381


class KVStore:
    """A single shared, flat, non-transactional key-value table.

    Keys handed to the public methods are *logical* keys within ``namespace``;
    internally they are prefixed the way GCS prefixes them, so that scans see a
    table that other components are also writing to.
    """

    def __init__(self, namespace: str = "ledger", foreign_keys: Optional[List[str]] = None):
        self.namespace = namespace
        self.data: Dict[str, Any] = {}
        self.fsyncs = 0
        self.reads = 0
        self.scanned_entries = 0
        # Other components share this table.  Serve, the Job manager, the
        # dashboard and the autoscaler are all in here in a real cluster.
        for fk in foreign_keys or [
            "@namespace_serve:ray-serve-default-checkpoint",
            "@namespace_job:JOB:raysubmit_abc",
            "@namespace_dashboard:agent_port",
        ]:
            self.data[fk] = b"foreign"

    # -- physical layout ---------------------------------------------------

    def _pk(self, key: str) -> str:
        return f"@namespace_{self.namespace}:{key}"

    # -- operations --------------------------------------------------------

    def put(self, key: str, value: Any, overwrite: bool = True) -> int:
        """Raw semantics: returns the number of keys *added* (1 or 0).

        With ``overwrite=False`` this is a compare-and-swap against "absent",
        which is the only atomic conditional the real store offers.
        """
        pk = self._pk(key)
        existed = pk in self.data
        if existed and not overwrite:
            self.fsyncs += 1  # the RPC still round-trips and still syncs
            return 0
        # deep-copied on the way in: values cross a process boundary for real,
        # so an aliasing bug must not be able to hide in this model.
        self.data[pk] = copy.deepcopy(value)
        self.fsyncs += 1
        return 0 if existed else 1

    def py_put(self, key: str, value: Any, overwrite: bool = True) -> bool:
        """``_internal_kv_put``'s semantics: True when the key ALREADY EXISTED."""
        return self.put(key, value, overwrite) == 0

    def get(self, key: str) -> Any:
        self.reads += 1
        return copy.deepcopy(self.data.get(self._pk(key)))

    def exists(self, key: str) -> bool:
        self.reads += 1
        return self._pk(key) in self.data

    def delete(self, key: str) -> int:
        pk = self._pk(key)
        self.fsyncs += 1
        if pk in self.data:
            del self.data[pk]
            return 1
        return 0

    def keys(self, prefix: str) -> List[str]:
        """Prefix scan.  Costs a walk of the whole shared table."""
        self.reads += 1
        self.scanned_entries += len(self.data)
        phys_prefix = self._pk(prefix)
        return sorted(
            pk[len(self._pk("")) :] for pk in self.data if pk.startswith(phys_prefix)
        )

    # -- introspection for invariants -------------------------------------

    def logical_keys(self) -> List[str]:
        own = self._pk("")
        return sorted(pk[len(own) :] for pk in self.data if pk.startswith(own))

    def snapshot(self) -> Dict[str, Any]:
        return copy.deepcopy(self.data)

    @property
    def fsync_seconds(self) -> float:
        return self.fsyncs * FSYNC_SECONDS
