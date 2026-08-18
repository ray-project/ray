import importlib.util
import os
import sys
import unittest

# Load module directly
file_path = os.path.join(
    os.path.dirname(__file__),
    "../production_debt.py",
)
spec = importlib.util.spec_from_file_location("ray_production_debt", file_path)
production_debt_mod = importlib.util.module_from_spec(spec)
sys.modules["ray_production_debt"] = production_debt_mod
spec.loader.exec_module(production_debt_mod)

ProductionDebtClusterGate = production_debt_mod.ProductionDebtClusterGate
TechnicalDueDiligenceLedger = production_debt_mod.TechnicalDueDiligenceLedger
GENESIS_HASH = production_debt_mod.GENESIS_HASH


class TestProductionDebtClusterGate(unittest.TestCase):
    def setUp(self) -> None:
        self.gate = ProductionDebtClusterGate(
            never_equate_intent_to_approval=True,
            max_acceptable_rdi=12.0,
        )

    def test_clean_cluster_execution_passes_readiness(self) -> None:
        report = self.gate.evaluate_cluster_execution(
            cluster_id="ray_vllm_100_node_cluster",
            allocated_plasma_bytes=50000000000,
            utilized_plasma_bytes=51500000000,
            distributed_task_latency_seconds=0.35,
            actor_placement_deadlocks=0,
            un_gated_mutations=0,
        )
        self.assertTrue(report.is_production_ready)
        self.assertLessEqual(report.rdi_score, 12.0)
        self.assertEqual(len(report.critical_smells), 0)
        self.assertTrue(bool(report.receipt_hash))

    def test_degraded_cluster_execution_fails_debt(self) -> None:
        report = self.gate.evaluate_cluster_execution(
            cluster_id="uncalibrated_plasma_spill_cluster",
            allocated_plasma_bytes=50000000000,
            utilized_plasma_bytes=130000000000,  # High plasma spill sprawl (2.6x)
            distributed_task_latency_seconds=3.2,  # High task latency
            actor_placement_deadlocks=3,  # 3 placement deadlocks
            un_gated_mutations=2,  # 2 un-gated mutations
        )
        self.assertFalse(report.is_production_ready)
        self.assertGreater(report.rdi_score, 50.0)
        self.assertIn("HIGH_PLASMA_STORE_SPILL_SPRAWL_2.60X", report.critical_smells)
        self.assertIn("HIGH_DISTRIBUTED_TASK_LATENCY_3.20S", report.critical_smells)
        self.assertIn("DETECTED_3_ACTOR_PLACEMENT_DEADLOCKS", report.critical_smells)
        self.assertIn("DETECTED_2_UNGATED_REMOTE_ACTOR_MUTATIONS", report.critical_smells)

    def test_cryptographic_ledger_integrity(self) -> None:
        self.gate.evaluate_cluster_execution("cluster-1")
        self.gate.evaluate_cluster_execution("cluster-2")
        self.gate.evaluate_cluster_execution("cluster-3")

        entries = self.gate.ledger.get_ledger_entries()
        self.assertEqual(len(entries), 3)
        self.assertEqual(entries[0]["prev_hash"], GENESIS_HASH)
        self.assertEqual(entries[1]["prev_hash"], entries[0]["curr_hash"])
        self.assertEqual(entries[2]["prev_hash"], entries[1]["curr_hash"])
        self.assertTrue(self.gate.ledger.verify_ledger_integrity())


if __name__ == "__main__":
    unittest.main()
