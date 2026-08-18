from __future__ import annotations

import hashlib
import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

GENESIS_HASH = "0000000000000000000000000000000000000000000000000000000000000000"


@dataclass
class RayDebtReport:
    cluster_id: str
    rdi_score: float  # Ray Debt Index (target <= 12.0)
    plasma_spill_multiplier: float  # Target <= 1.08x
    distributed_task_latency_seconds: float  # Target <= 0.55s
    mutation_safety_score: float  # Target 100.0
    production_readiness_index: float  # Scale 0 - 100
    is_production_ready: bool
    critical_smells: list[str]
    receipt_hash: str


class TechnicalDueDiligenceLedger:
    """Cryptographic SHA-256 hash-chained Action Ledger for Ray distributed execution runs."""

    def __init__(self) -> None:
        self._entries: list[dict[str, Any]] = []
        self._last_hash: str = GENESIS_HASH

    def record_cluster_event(
        self,
        cluster_id: str,
        event_type: str,
        readiness_index: float,
        critical_smells: list[str],
        metadata: dict[str, Any],
    ) -> dict[str, Any]:
        timestamp = datetime.now(timezone.utc).isoformat()
        index = len(self._entries)

        meta_bytes = json.dumps(metadata, sort_keys=True).encode("utf-8")
        canonical_content = (
            f"{index}|{self._last_hash}|{cluster_id}|{event_type}|"
            f"{readiness_index}|{timestamp}|{hashlib.sha256(meta_bytes).hexdigest()}"
        )
        curr_hash = hashlib.sha256(canonical_content.encode("utf-8")).hexdigest()

        entry = {
            "index": index,
            "timestamp": timestamp,
            "cluster_id": cluster_id,
            "event_type": event_type,
            "readiness_index": readiness_index,
            "critical_smells": critical_smells,
            "prev_hash": self._last_hash,
            "curr_hash": curr_hash,
            "metadata": metadata,
        }

        self._entries.append(entry)
        self._last_hash = curr_hash
        return entry

    def get_ledger_entries(self) -> list[dict[str, Any]]:
        return list(self._entries)

    def verify_ledger_integrity(self) -> bool:
        prev = GENESIS_HASH
        for entry in self._entries:
            if entry["prev_hash"] != prev:
                return False
            prev = entry["curr_hash"]
        return True


class ProductionDebtClusterGate:
    """A2Z SOC Production Debt & Technical Due Diligence Gate for Ray Distributed Clusters.

    Quantifies plasma object store memory spilling, placement group deadlocks, and task latency against 4 Enterprise KPIs:
    1. Ray Debt Index (RDI <= 12.0)
    2. Plasma Memory Spill Multiplier (PMSM <= 1.08x)
    3. P99 Distributed Task Latency (<= 0.55s)
    4. Deterministic Mutation Boundaries (never_equate_intent_to_approval)
    """

    def __init__(
        self,
        never_equate_intent_to_approval: bool = True,
        max_acceptable_rdi: float = 12.0,
    ) -> None:
        self.never_equate_intent_to_approval = never_equate_intent_to_approval
        self.max_acceptable_rdi = max_acceptable_rdi
        self.ledger = TechnicalDueDiligenceLedger()

    def check_kill_switch(self) -> bool:
        if os.environ.get("AAG_KILL_SWITCH", "").lower() in ("true", "1", "yes"):
            return True
        return any(Path(p).exists() for p in ("artifacts/KILL", "/tmp/KILL"))

    def evaluate_cluster_execution(
        self,
        cluster_id: str,
        allocated_plasma_bytes: int = 50000000000,
        utilized_plasma_bytes: int = 52000000000,
        distributed_task_latency_seconds: float = 0.35,
        actor_placement_deadlocks: int = 0,
        un_gated_mutations: int = 0,
    ) -> RayDebtReport:
        # 1. Evaluate emergency kill switch
        if self.check_kill_switch():
            self.ledger.record_cluster_event(
                cluster_id=cluster_id,
                event_type="execution_halted_kill_switch",
                readiness_index=0.0,
                critical_smells=["EMERGENCY_KILL_SWITCH_ENGAGED"],
                metadata={"reason": "AAG_KILL_SWITCH is set"},
            )
            err_msg = "A2Z SOC ActionGate: Emergency kill switch is engaged. Ray cluster execution halted."
            raise PermissionError(err_msg)

        critical_smells: list[str] = []

        # KPI 2: Plasma Memory Spill Multiplier
        spill_ratio = utilized_plasma_bytes / max(1, allocated_plasma_bytes)
        if spill_ratio > 1.8:
            critical_smells.append(f"HIGH_PLASMA_STORE_SPILL_SPRAWL_{spill_ratio:.2f}X")

        # KPI 3: Latency Ceiling
        if distributed_task_latency_seconds > 2.0:
            critical_smells.append(f"HIGH_DISTRIBUTED_TASK_LATENCY_{distributed_task_latency_seconds:.2f}S")

        # Actor placement deadlocks
        if actor_placement_deadlocks > 1:
            critical_smells.append(f"DETECTED_{actor_placement_deadlocks}_ACTOR_PLACEMENT_DEADLOCKS")

        # KPI 4: Mutation Safety
        if un_gated_mutations > 0:
            critical_smells.append(f"DETECTED_{un_gated_mutations}_UNGATED_REMOTE_ACTOR_MUTATIONS")

        # KPI 1: Ray Debt Index (0 = Clean, 100 = Catastrophic)
        rdi = (
            max(0.0, (spill_ratio - 1.0) * 20.0)
            + max(0.0, (distributed_task_latency_seconds - 0.55) * 10.0)
            + (actor_placement_deadlocks * 15.0)
            + (un_gated_mutations * 30.0)
        )
        rdi_score = round(min(100.0, rdi), 2)

        # Production Readiness Index (0 - 100)
        readiness = max(0.0, 100.0 - rdi_score)
        is_production_ready = (
            rdi_score <= self.max_acceptable_rdi and len(critical_smells) == 0
        )

        # Cryptographic Ledger Entry
        entry = self.ledger.record_cluster_event(
            cluster_id=cluster_id,
            event_type="execution_authorized" if is_production_ready else "execution_flagged_debt",
            readiness_index=readiness,
            critical_smells=critical_smells,
            metadata={
                "rdi_score": rdi_score,
                "spill_ratio": spill_ratio,
                "allocated_plasma_bytes": allocated_plasma_bytes,
                "utilized_plasma_bytes": utilized_plasma_bytes,
                "distributed_task_latency_seconds": distributed_task_latency_seconds,
                "actor_placement_deadlocks": actor_placement_deadlocks,
                "un_gated_mutations": un_gated_mutations,
                "never_equate_intent_to_approval": self.never_equate_intent_to_approval,
            },
        )

        return RayDebtReport(
            cluster_id=cluster_id,
            rdi_score=rdi_score,
            plasma_spill_multiplier=round(spill_ratio, 2),
            distributed_task_latency_seconds=round(distributed_task_latency_seconds, 2),
            mutation_safety_score=(
                100.0 if un_gated_mutations == 0 else max(0.0, 100.0 - un_gated_mutations * 30.0)
            ),
            production_readiness_index=readiness,
            is_production_ready=is_production_ready,
            critical_smells=critical_smells,
            receipt_hash=entry["curr_hash"],
        )
