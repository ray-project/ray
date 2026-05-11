import sys
from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError

from ray.serve._private.common import CreatePlacementGroupRequest
from ray.serve._private.default_impl import (
    ReplicaPlacementGroup,
    _create_replica_placement_group,
)
from ray.serve.api import deployment
from ray.serve.config import TPUAcceleratorConfig
from ray.util.placement_group import PlacementGroup
from ray.util.tpu import SlicePlacementGroup


def test_tpu_accelerator_config_construction():
    config = TPUAcceleratorConfig(topology="4x4", accelerator_version="v6e")
    assert config.kind == "tpu"
    assert config.topology == "4x4"
    assert config.num_slices == 1  # default


def test_tpu_accelerator_config_immutable():
    config = TPUAcceleratorConfig(topology="4x4", accelerator_version="v6e")
    with pytest.raises(ValidationError):
        config.topology = "2x2"


def test_tpu_accelerator_config_extra_forbid():
    with pytest.raises(ValidationError):
        TPUAcceleratorConfig(topology="4x4", accelerator_version="v6e", bogus_field=1)


def test_deployment_options_accept_tpu_config_instance():
    config = TPUAcceleratorConfig(topology="4x4", accelerator_version="v6e")

    @deployment(accelerator_config=config)
    class D:
        pass

    assert isinstance(D._deployment_config.accelerator_config, TPUAcceleratorConfig)


def test_deployment_options_accept_dict_form():
    @deployment(
        accelerator_config={
            "kind": "tpu",
            "topology": "4x4",
            "accelerator_version": "v6e",
        }
    )
    class D:
        pass

    cfg = D._deployment_config.accelerator_config
    assert isinstance(cfg, TPUAcceleratorConfig)
    assert cfg.topology == "4x4"


def test_deployment_options_dict_unknown_accelerator_type_raises():
    with pytest.raises(ValueError, match="Unknown accelerator kind"):

        @deployment(accelerator_config={"kind": "xpu"})
        class D:
            pass


@pytest.mark.parametrize(
    "invalid_kwargs",
    [
        {"topology": "4x4"},  # missing accelerator_version
        {"accelerator_version": "v6e"},  # missing topology
        {"topology": 123, "accelerator_version": "v6e"},  # topology should be str
        {
            "topology": "4x4",
            "accelerator_version": "v6e",
            "num_slices": "two",
        },  # num_slices should be int
        {
            "topology": "4x4",
            "accelerator_version": "v6e",
            "num_slices": 0,
        },  # num_slices must be >= 1
    ],
)
def test_tpu_accelerator_config_validation(invalid_kwargs):
    with pytest.raises(ValidationError):
        TPUAcceleratorConfig(**invalid_kwargs)


@pytest.mark.parametrize("with_accelerator", [False, True])
def test_placement_group_creation_types(with_accelerator):
    """Verify that _create_replica_placement_group always returns wrappers."""

    accelerator_config = None
    if with_accelerator:
        accelerator_config = TPUAcceleratorConfig(
            topology="4x4", accelerator_version="v6e"
        )

    request = CreatePlacementGroupRequest(
        bundles=[{"CPU": 1.0}],
        strategy="SPREAD",
        target_node_id="",
        name="test",
        accelerator_config=accelerator_config,
    )

    mock_pg = MagicMock(spec=PlacementGroup)

    # Accelerator path. Returns a wrapper holding a SlicePlacementGroup.
    if with_accelerator:
        mock_slice_pg = MagicMock()
        mock_slice_pg.placement_group = mock_pg
        with patch(
            "ray.serve._private.default_impl.slice_placement_group",
            return_value=mock_slice_pg,
        ):
            result = _create_replica_placement_group(request)
    # Non-accelerator path. Returns a wrapper holding a regular PG.
    else:
        with patch("ray.util.placement_group", return_value=mock_pg):
            result = _create_replica_placement_group(request)

    assert isinstance(result, ReplicaPlacementGroup), (
        "_create_replica_placement_group must always return a ReplicaPlacementGroup, "
        "regardless of whether accelerator_config is set."
    )
    assert result.placement_group == mock_pg

    if with_accelerator:
        assert (
            result._slice_pg is not None
        ), "Accelerator path must set _slice_pg for cleanup tracking."
    else:
        assert result._slice_pg is None, "Non-accelerator path must not set _slice_pg."


@pytest.mark.parametrize("with_accelerator", [False, True])
def test_replica_pg_shutdown_idempotent(with_accelerator):
    """Test that ReplicaPlacementGroup shutdown is idempotent."""
    mock_pg = MagicMock()

    if with_accelerator:
        mock_slice_pg = MagicMock()
        adapter = ReplicaPlacementGroup(
            placement_group=mock_pg, _slice_pg=mock_slice_pg
        )

        adapter.shutdown()
        mock_slice_pg.shutdown.assert_called_once()
        assert adapter._slice_pg is None

        adapter.shutdown()
        assert mock_slice_pg.shutdown.call_count == 1
    else:
        adapter = ReplicaPlacementGroup(placement_group=mock_pg)

        with patch(
            "ray.serve._private.default_impl.remove_placement_group"
        ) as mock_remove:
            adapter.shutdown()
            mock_remove.assert_called_once_with(mock_pg)

            adapter.shutdown()
            assert mock_remove.call_count == 1


def test_create_replica_placement_group_rejects_no_bundles_no_config():
    """Without bundles or a recognized accelerator_config, raises ValueError.

    Catches future accelerator types added to AcceleratorConfig but not
    wired into _create_replica_placement_group.
    """
    request = CreatePlacementGroupRequest(
        bundles=None,
        strategy="PACK",
        target_node_id="",
        name="test",
        accelerator_config=None,
    )
    with pytest.raises(ValueError, match="requires either non-None bundles"):
        _create_replica_placement_group(request)


def test_create_replica_placement_group_tpu_ignores_bundles():
    """TPU dispatch ignores request.bundles -- they're derived from topology."""
    request = CreatePlacementGroupRequest(
        bundles=[{"CPU": 1}],
        strategy="PACK",
        target_node_id="",
        name="test",
        accelerator_config=TPUAcceleratorConfig(
            topology="2x2", accelerator_version="v6e"
        ),
    )

    mock_slice_pg = MagicMock()
    mock_slice_pg.placement_group = MagicMock(spec=PlacementGroup)

    with patch(
        "ray.serve._private.default_impl.slice_placement_group",
        return_value=mock_slice_pg,
    ) as mock_slice_pg_func:
        result = _create_replica_placement_group(request)

        mock_slice_pg_func.assert_called_once()

    assert result.placement_group == mock_slice_pg.placement_group


def test_tpu_config_resources_per_bundle_forwarded_to_slice_pg(monkeypatch):
    """The resources_per_bundle field is forwarded to slice_placement_group."""
    captured = {}

    # Mock slice_placement_group to capture the arguments it receives.
    def mock_slice_pg(**kwargs):
        captured.update(kwargs)
        mock = MagicMock(spec=SlicePlacementGroup)
        mock.placement_group = MagicMock()
        return mock

    monkeypatch.setattr(
        "ray.serve._private.default_impl.slice_placement_group",
        mock_slice_pg,
    )

    # Create a config with custom resources per bundle.
    config = TPUAcceleratorConfig(
        topology="4x4",
        accelerator_version="v6e",
        resources_per_bundle={"TPU": 1, "memory": 1_000_000},
    )
    request = CreatePlacementGroupRequest(
        accelerator_config=config,
        name="test",
    )

    # Call the dispatch function.
    _create_replica_placement_group(request)

    # Verify that resources_per_bundle and other fields were forwarded correctly.
    assert captured["resources_per_bundle"] == {"TPU": 1, "memory": 1_000_000}
    assert captured["topology"] == "4x4"
    assert captured["accelerator_version"] == "v6e"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
