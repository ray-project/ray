"""Tests for Autoscaler._autoscaling_config_to_cluster_config and
Autoscaler._report_cluster_config.
"""
import os
import sys
from unittest.mock import MagicMock, patch

import pytest

from ray.autoscaler.v2.autoscaler import Autoscaler
from ray.autoscaler.v2.instance_manager.config import AutoscalingConfig
from ray.core.generated.autoscaler_pb2 import ClusterConfig

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_autoscaling_config(config_dict: dict) -> AutoscalingConfig:
    """Build an AutoscalingConfig from a raw dict, skipping content hashing."""
    return AutoscalingConfig(configs=config_dict, skip_content_hash=True)


def _base_config(**overrides) -> dict:
    """Minimal valid autoscaling config dict."""
    cfg = {
        "cluster_name": "test-cluster",
        "max_workers": 10,
        "provider": {"type": "kuberay", "namespace": "default"},
        "head_node_type": "headgroup",
        "available_node_types": {
            "headgroup": {
                "resources": {"CPU": 4, "memory": 8000000000},
                "min_workers": 0,
                "max_workers": 0,
                "node_config": {},
            },
        },
        # Legacy fields required by validate_config.
        "file_mounts": {},
        "cluster_synced_files": [],
        "file_mounts_sync_continuously": False,
        "initialization_commands": [],
        "setup_commands": [],
        "head_setup_commands": [],
        "worker_setup_commands": [],
        "head_start_ray_commands": [],
        "worker_start_ray_commands": [],
        "auth": {},
    }
    cfg.update(overrides)
    return cfg


# ---------------------------------------------------------------------------
# Tests for _autoscaling_config_to_cluster_config
# ---------------------------------------------------------------------------


class TestAutoscalingConfigToClusterConfig:
    """Unit tests for the static conversion method (no Ray cluster needed)."""

    def test_empty_node_types(self):
        """Config with only a head node produces an empty node_group_configs."""
        config = _make_autoscaling_config(_base_config())
        result = Autoscaler._autoscaling_config_to_cluster_config(config)

        assert isinstance(result, ClusterConfig)
        # Head is excluded, no worker groups → empty.
        assert len(result.node_group_configs) == 0

    def test_single_worker_group(self):
        """One worker group is correctly converted."""
        raw = _base_config()
        raw["available_node_types"]["worker-cpu"] = {
            "resources": {"CPU": 8, "memory": 16000000000},
            "min_workers": 1,
            "max_workers": 5,
            "node_config": {},
        }
        raw["max_workers"] = 5
        config = _make_autoscaling_config(raw)
        result = Autoscaler._autoscaling_config_to_cluster_config(config)

        assert len(result.node_group_configs) == 1
        ngc = result.node_group_configs[0]
        assert ngc.name == "worker-cpu"
        assert ngc.resources["CPU"] == 8
        assert ngc.resources["memory"] == 16000000000
        assert ngc.min_count == 1
        assert ngc.max_count == 5

    def test_multiple_worker_groups(self):
        """Multiple worker groups are all included."""
        raw = _base_config()
        raw["available_node_types"]["worker-cpu"] = {
            "resources": {"CPU": 4},
            "min_workers": 0,
            "max_workers": 10,
            "node_config": {},
        }
        raw["available_node_types"]["worker-gpu"] = {
            "resources": {"CPU": 8, "GPU": 2, "memory": 32000000000},
            "min_workers": 1,
            "max_workers": 3,
            "node_config": {},
        }
        raw["max_workers"] = 13
        config = _make_autoscaling_config(raw)
        result = Autoscaler._autoscaling_config_to_cluster_config(config)

        assert len(result.node_group_configs) == 2
        names = {ngc.name for ngc in result.node_group_configs}
        assert names == {"worker-cpu", "worker-gpu"}

        by_name = {ngc.name: ngc for ngc in result.node_group_configs}
        cpu_ngc = by_name["worker-cpu"]
        assert cpu_ngc.resources["CPU"] == 4
        assert cpu_ngc.min_count == 0
        assert cpu_ngc.max_count == 10

        gpu_ngc = by_name["worker-gpu"]
        assert gpu_ngc.resources["CPU"] == 8
        assert gpu_ngc.resources["GPU"] == 2
        assert gpu_ngc.resources["memory"] == 32000000000
        assert gpu_ngc.min_count == 1
        assert gpu_ngc.max_count == 3

    def test_head_node_excluded(self):
        """The head node type must NOT appear in node_group_configs."""
        raw = _base_config()
        raw["available_node_types"]["worker-cpu"] = {
            "resources": {"CPU": 2},
            "min_workers": 0,
            "max_workers": 5,
            "node_config": {},
        }
        raw["max_workers"] = 5
        config = _make_autoscaling_config(raw)
        result = Autoscaler._autoscaling_config_to_cluster_config(config)

        names = [ngc.name for ngc in result.node_group_configs]
        assert "headgroup" not in names
        assert len(names) == 1
        assert names[0] == "worker-cpu"

    def test_float_resources_truncated_to_int(self):
        """Float resource values should be truncated to int (uint64).

        We bypass AutoscalingConfig validation (which requires integer resources)
        and test the conversion logic directly by mocking get_node_type_configs.
        """
        from ray.autoscaler.v2.instance_manager.config import NodeTypeConfig

        raw = _base_config()
        config = _make_autoscaling_config(raw)

        # Simulate a config where resources happen to be float
        # (e.g. from programmatic construction, not schema-validated input).
        mock_node_types = {
            "headgroup": NodeTypeConfig(
                name="headgroup",
                min_worker_nodes=0,
                max_worker_nodes=1,
                resources={"CPU": 4.0},
            ),
            "worker": NodeTypeConfig(
                name="worker",
                min_worker_nodes=0,
                max_worker_nodes=2,
                resources={"CPU": 3.7, "GPU": 1.9},
            ),
        }
        with patch.object(
            config, "get_node_type_configs", return_value=mock_node_types
        ), patch.object(config, "get_head_node_type", return_value="headgroup"):
            result = Autoscaler._autoscaling_config_to_cluster_config(config)

        assert len(result.node_group_configs) == 1
        ngc = result.node_group_configs[0]
        # int(3.7) = 3, int(1.9) = 1
        assert ngc.resources["CPU"] == 3
        assert ngc.resources["GPU"] == 1

    def test_max_resources_not_set(self):
        """ClusterConfig.max_resources should be empty (not set)."""
        raw = _base_config()
        raw["available_node_types"]["worker"] = {
            "resources": {"CPU": 4},
            "min_workers": 0,
            "max_workers": 5,
            "node_config": {},
        }
        raw["max_workers"] = 5
        config = _make_autoscaling_config(raw)
        result = Autoscaler._autoscaling_config_to_cluster_config(config)

        assert len(result.max_resources) == 0
        assert len(result.min_resources) == 0

    def test_custom_resources_included(self):
        """Custom resources (e.g. TPU, neuron_cores) should be carried through."""
        raw = _base_config()
        raw["available_node_types"]["worker-tpu"] = {
            "resources": {"CPU": 4, "TPU": 8, "neuron_cores": 16},
            "min_workers": 0,
            "max_workers": 2,
            "node_config": {},
        }
        raw["max_workers"] = 2
        config = _make_autoscaling_config(raw)
        result = Autoscaler._autoscaling_config_to_cluster_config(config)

        ngc = result.node_group_configs[0]
        assert ngc.resources["CPU"] == 4
        assert ngc.resources["TPU"] == 8
        assert ngc.resources["neuron_cores"] == 16

    def test_zero_max_workers_excluded(self):
        """A worker group with max_workers=0 should still appear
        (get_node_type_configs does not filter it), but with max_count=0
        so the reading side skips it."""
        raw = _base_config()
        raw["available_node_types"]["worker-disabled"] = {
            "resources": {"CPU": 2},
            "min_workers": 0,
            "max_workers": 0,
            "node_config": {},
        }
        raw["max_workers"] = 0
        config = _make_autoscaling_config(raw)
        result = Autoscaler._autoscaling_config_to_cluster_config(config)

        # It shows up in node_group_configs but with max_count=0,
        # so _get_node_resource_spec_and_count will skip it.
        ngc = result.node_group_configs[0]
        assert ngc.max_count == 0

    def test_serialization_roundtrip(self):
        """Verify the protobuf can be serialized and deserialized correctly."""
        raw = _base_config()
        raw["available_node_types"]["worker-cpu"] = {
            "resources": {"CPU": 8, "memory": 16000000000},
            "min_workers": 2,
            "max_workers": 10,
            "node_config": {},
        }
        raw["max_workers"] = 10
        config = _make_autoscaling_config(raw)
        original = Autoscaler._autoscaling_config_to_cluster_config(config)

        serialized = original.SerializeToString()
        restored = ClusterConfig.FromString(serialized)

        assert restored == original
        assert len(restored.node_group_configs) == 1
        ngc = restored.node_group_configs[0]
        assert ngc.name == "worker-cpu"
        assert ngc.resources["CPU"] == 8
        assert ngc.min_count == 2
        assert ngc.max_count == 10


# ---------------------------------------------------------------------------
# Tests for _report_cluster_config
# ---------------------------------------------------------------------------


class TestReportClusterConfig:
    """Tests for the _report_cluster_config method using a mock GCS client."""

    def _make_autoscaler_with_mock_gcs(self, raw_config: dict):
        """Create an Autoscaler with everything mocked except the conversion logic."""
        config = _make_autoscaling_config(raw_config)

        mock_config_reader = MagicMock()
        mock_config_reader.get_cached_autoscaling_config.return_value = config

        mock_gcs_client = MagicMock()

        # Patch out heavy init: cloud provider, instance manager, scheduler
        with patch.object(Autoscaler, "_init_cloud_instance_provider"), patch.object(
            Autoscaler, "_init_instance_manager"
        ):
            autoscaler = Autoscaler(
                session_name="test-session",
                config_reader=mock_config_reader,
                gcs_client=mock_gcs_client,
            )

        return autoscaler, mock_gcs_client

    def test_report_called_on_init(self):
        """report_cluster_config should be called during __init__."""
        raw = _base_config()
        raw["available_node_types"]["worker"] = {
            "resources": {"CPU": 4},
            "min_workers": 0,
            "max_workers": 5,
            "node_config": {},
        }
        raw["max_workers"] = 5

        autoscaler, mock_gcs = self._make_autoscaler_with_mock_gcs(raw)

        # report_cluster_config is called exactly once during __init__.
        assert mock_gcs.report_cluster_config.call_count == 1

        # Verify the serialized data is correct.
        serialized = mock_gcs.report_cluster_config.call_args[0][0]
        restored = ClusterConfig.FromString(serialized)
        assert len(restored.node_group_configs) == 1
        assert restored.node_group_configs[0].name == "worker"
        assert restored.node_group_configs[0].resources["CPU"] == 4
        assert restored.node_group_configs[0].max_count == 5

    def test_report_called_on_update(self):
        """report_cluster_config should be called in update_autoscaling_state
        when the config has changed since the last report."""
        raw_init = _base_config()
        raw_init["available_node_types"]["worker"] = {
            "resources": {"CPU": 2},
            "min_workers": 0,
            "max_workers": 3,
            "node_config": {},
        }
        raw_init["max_workers"] = 3

        autoscaler, mock_gcs = self._make_autoscaler_with_mock_gcs(raw_init)

        # Provide mocks for attributes that are None because init was patched.
        autoscaler._cloud_instance_provider = MagicMock()
        autoscaler._instance_manager = MagicMock()
        autoscaler._cloud_resource_monitor = MagicMock()

        # Reset after init call.
        mock_gcs.report_cluster_config.reset_mock()

        # Use a *different* config for the update so that the caching logic
        # does not skip the report.
        raw_updated = _base_config()
        raw_updated["available_node_types"]["worker"] = {
            "resources": {"CPU": 4},
            "min_workers": 1,
            "max_workers": 8,
            "node_config": {},
        }
        raw_updated["max_workers"] = 8

        # Mock the dependencies of update_autoscaling_state.
        with patch.object(autoscaler, "_config_reader") as mock_reader, patch(
            "ray.autoscaler.v2.autoscaler.get_cluster_resource_state"
        ), patch("ray.autoscaler.v2.autoscaler.Reconciler") as mock_reconciler:
            mock_reader.get_cached_autoscaling_config.return_value = (
                _make_autoscaling_config(raw_updated)
            )
            mock_reconciler.reconcile.return_value = MagicMock()

            result = autoscaler.update_autoscaling_state()

        # Verify update_autoscaling_state completed successfully.
        assert result is not None
        # One call from update_autoscaling_state (config changed).
        assert mock_gcs.report_cluster_config.call_count == 1

        # Verify the reported data reflects the updated config.
        serialized = mock_gcs.report_cluster_config.call_args[0][0]
        restored = ClusterConfig.FromString(serialized)
        assert restored.node_group_configs[0].resources["CPU"] == 4
        assert restored.node_group_configs[0].min_count == 1
        assert restored.node_group_configs[0].max_count == 8

    def test_report_skipped_when_config_unchanged(self):
        """If the config has not changed since the last report, the GCS call
        should be skipped (caching dedup)."""
        raw = _base_config()
        raw["available_node_types"]["worker"] = {
            "resources": {"CPU": 2},
            "min_workers": 0,
            "max_workers": 3,
            "node_config": {},
        }
        raw["max_workers"] = 3

        autoscaler, mock_gcs = self._make_autoscaler_with_mock_gcs(raw)

        autoscaler._cloud_instance_provider = MagicMock()
        autoscaler._instance_manager = MagicMock()
        autoscaler._cloud_resource_monitor = MagicMock()

        # __init__ reported once.
        assert mock_gcs.report_cluster_config.call_count == 1
        mock_gcs.report_cluster_config.reset_mock()

        # Run update_autoscaling_state with the *same* config.
        with patch.object(autoscaler, "_config_reader") as mock_reader, patch(
            "ray.autoscaler.v2.autoscaler.get_cluster_resource_state"
        ), patch("ray.autoscaler.v2.autoscaler.Reconciler") as mock_reconciler:
            mock_reader.get_cached_autoscaling_config.return_value = (
                _make_autoscaling_config(raw)
            )
            mock_reconciler.reconcile.return_value = MagicMock()

            result = autoscaler.update_autoscaling_state()

        assert result is not None
        # The config is identical to what was reported during init,
        # so report_cluster_config should NOT be called again.
        assert mock_gcs.report_cluster_config.call_count == 0

    def test_cache_not_updated_on_gcs_error(self):
        """If gcs_client.report_cluster_config raises, the cache should NOT be
        updated, so the next call with the same config will retry."""
        raw = _base_config()
        raw["available_node_types"]["worker"] = {
            "resources": {"CPU": 2},
            "min_workers": 0,
            "max_workers": 3,
            "node_config": {},
        }
        raw["max_workers"] = 3

        config = _make_autoscaling_config(raw)
        mock_gcs = MagicMock()
        # First call raises, simulating GCS unavailable.
        mock_gcs.report_cluster_config.side_effect = RuntimeError("GCS unavailable")

        mock_config_reader = MagicMock()
        mock_config_reader.get_cached_autoscaling_config.return_value = config

        with patch.object(Autoscaler, "_init_cloud_instance_provider"), patch.object(
            Autoscaler, "_init_instance_manager"
        ):
            autoscaler = Autoscaler(
                session_name="test-session",
                config_reader=mock_config_reader,
                gcs_client=mock_gcs,
            )

        # The init call attempted to report but failed.
        assert mock_gcs.report_cluster_config.call_count == 1
        # Cache should still be None because the report failed.
        assert autoscaler._last_reported_cluster_config is None

        # Now GCS recovers.
        mock_gcs.report_cluster_config.reset_mock()
        mock_gcs.report_cluster_config.side_effect = None

        # Manually call _report_cluster_config with the same config.
        autoscaler._report_cluster_config(config)

        # This time it should succeed and call GCS (not skipped by cache).
        assert mock_gcs.report_cluster_config.call_count == 1
        # Cache should now be updated.
        assert autoscaler._last_reported_cluster_config is not None

    def test_report_survives_gcs_error(self):
        """If gcs_client.report_cluster_config raises, it should be caught."""
        raw = _base_config()
        raw["available_node_types"]["worker"] = {
            "resources": {"CPU": 2},
            "min_workers": 0,
            "max_workers": 3,
            "node_config": {},
        }
        raw["max_workers"] = 3

        config = _make_autoscaling_config(raw)
        mock_gcs = MagicMock()
        mock_gcs.report_cluster_config.side_effect = RuntimeError("GCS unavailable")

        mock_config_reader = MagicMock()
        mock_config_reader.get_cached_autoscaling_config.return_value = config

        # Should NOT raise during __init__ despite GCS error.
        with patch.object(Autoscaler, "_init_cloud_instance_provider"), patch.object(
            Autoscaler, "_init_instance_manager"
        ):
            autoscaler = Autoscaler(
                session_name="test-session",
                config_reader=mock_config_reader,
                gcs_client=mock_gcs,
            )

        # The autoscaler object should still be created.
        assert autoscaler is not None

    def test_report_reflects_config_refresh(self):
        """After config refresh, the reported config should reflect changes."""
        raw_v1 = _base_config()
        raw_v1["available_node_types"]["worker"] = {
            "resources": {"CPU": 2},
            "min_workers": 0,
            "max_workers": 3,
            "node_config": {},
        }
        raw_v1["max_workers"] = 3

        raw_v2 = _base_config()
        raw_v2["available_node_types"]["worker"] = {
            "resources": {"CPU": 4},
            "min_workers": 1,
            "max_workers": 10,
            "node_config": {},
        }
        raw_v2["max_workers"] = 10

        autoscaler, mock_gcs = self._make_autoscaler_with_mock_gcs(raw_v1)

        # Provide mocks for attributes that are None because init was patched.
        autoscaler._cloud_instance_provider = MagicMock()
        autoscaler._instance_manager = MagicMock()
        autoscaler._cloud_resource_monitor = MagicMock()

        # First call (from init): verify v1 config.
        serialized_v1 = mock_gcs.report_cluster_config.call_args[0][0]
        restored_v1 = ClusterConfig.FromString(serialized_v1)
        assert restored_v1.node_group_configs[0].resources["CPU"] == 2
        assert restored_v1.node_group_configs[0].max_count == 3

        # Now simulate config refresh with v2.
        mock_gcs.report_cluster_config.reset_mock()
        config_v2 = _make_autoscaling_config(raw_v2)

        with patch.object(autoscaler, "_config_reader") as mock_reader, patch(
            "ray.autoscaler.v2.autoscaler.get_cluster_resource_state"
        ), patch("ray.autoscaler.v2.autoscaler.Reconciler") as mock_reconciler:
            mock_reader.get_cached_autoscaling_config.return_value = config_v2
            mock_reconciler.reconcile.return_value = MagicMock()
            result = autoscaler.update_autoscaling_state()

        # Verify update completed successfully.
        assert result is not None

        # Verify v2 config was reported.
        serialized_v2 = mock_gcs.report_cluster_config.call_args[0][0]
        restored_v2 = ClusterConfig.FromString(serialized_v2)
        assert restored_v2.node_group_configs[0].resources["CPU"] == 4
        assert restored_v2.node_group_configs[0].min_count == 1
        assert restored_v2.node_group_configs[0].max_count == 10


# ---------------------------------------------------------------------------
# Tests for compatibility with reading-side consumers
# ---------------------------------------------------------------------------


class TestReadingSideCompatibility:
    """Verify that the generated ClusterConfig protobuf is compatible with
    downstream consumers like _get_node_resource_spec_and_count and
    is_autoscaling_enabled.
    """

    def test_is_autoscaling_enabled_workers_only(self):
        """Simulate is_autoscaling_enabled check: worker groups with
        max_count > min_count should be detected as autoscalable."""
        raw = _base_config()
        raw["available_node_types"]["worker"] = {
            "resources": {"CPU": 4},
            "min_workers": 0,
            "max_workers": 5,
            "node_config": {},
        }
        raw["max_workers"] = 5
        config = _make_autoscaling_config(raw)
        cluster_config = Autoscaler._autoscaling_config_to_cluster_config(config)

        # Replicate the logic of is_autoscaling_enabled.
        autoscalable = any(
            ngc.max_count == -1 or ngc.max_count > ngc.min_count
            for ngc in cluster_config.node_group_configs
            if ngc.resources and ngc.max_count != 0
        )
        assert autoscalable is True

    def test_is_autoscaling_enabled_no_workers(self):
        """Head-only cluster should NOT be autoscalable."""
        config = _make_autoscaling_config(_base_config())
        cluster_config = Autoscaler._autoscaling_config_to_cluster_config(config)

        autoscalable = any(
            ngc.max_count == -1 or ngc.max_count > ngc.min_count
            for ngc in cluster_config.node_group_configs
            if ngc.resources and ngc.max_count != 0
        )
        assert autoscalable is False

    def test_max_resources_defaults_to_uncapped(self):
        """Empty max_resources should behave as uncapped (sys.maxsize)."""
        import sys

        raw = _base_config()
        raw["available_node_types"]["worker"] = {
            "resources": {"CPU": 4},
            "min_workers": 0,
            "max_workers": 5,
            "node_config": {},
        }
        raw["max_workers"] = 5
        config = _make_autoscaling_config(raw)
        cluster_config = Autoscaler._autoscaling_config_to_cluster_config(config)

        # Replicate _calculate_max_resource_from_cluster_config logic.
        max_cpu = 0
        for ngc in cluster_config.node_group_configs:
            num_resources = ngc.resources.get("CPU", 0)
            num_nodes = ngc.max_count
            if num_nodes == 0 or num_resources == 0:
                continue
            if num_nodes == -1:
                max_cpu = sys.maxsize
                break
            max_cpu += num_nodes * num_resources

        # Should be 5 * 4 = 20 for the worker group.
        assert max_cpu == 20

        # max_resources cap: empty map → sys.maxsize per key.
        cap = cluster_config.max_resources.get("CPU", sys.maxsize)
        assert cap == sys.maxsize
        assert min(max_cpu, cap) == 20


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
