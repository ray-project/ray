from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError

from ray.serve._private.common import CreatePlacementGroupRequest
from ray.serve._private.default_impl import (
    _create_replica_placement_group,
    _ReplicaPlacementGroup,
)
from ray.serve.api import deployment
from ray.serve.config import TPUAcceleratorConfig


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


def test_create_replica_placement_group_tpu_dispatch():
    config = TPUAcceleratorConfig(topology="4x4", accelerator_version="v6e")
    request = CreatePlacementGroupRequest(
        bundles=[],
        strategy="SPREAD",
        target_node_id="",
        name="test",
        accelerator_config=config,
    )

    fake_slice_pg = MagicMock()
    fake_slice_pg.placement_group = MagicMock()

    with patch(
        "ray.serve._private.default_impl.slice_placement_group"
    ) as mock_slice_pg:
        mock_slice_pg.return_value = fake_slice_pg

        result = _create_replica_placement_group(request)

        assert mock_slice_pg.called
        assert result._slice_pg is not None
        assert result.placement_group == fake_slice_pg.placement_group
        mock_slice_pg.assert_called_once()


def test_replica_pg_shutdown_idempotent():
    """Test that _ReplicaPlacementGroup shutdown is idempotent."""
    # Path 1: No accelerator
    mock_pg = MagicMock()
    adapter = _ReplicaPlacementGroup(placement_group=mock_pg)

    with patch("ray.serve._private.default_impl.remove_placement_group") as mock_remove:
        adapter.shutdown()
        mock_remove.assert_called_once_with(mock_pg)

        # Call again, should not raise or call remove again
        adapter.shutdown()
        assert mock_remove.call_count == 1

    # Path 2: With accelerator
    mock_slice_pg = MagicMock()
    adapter_with_accel = _ReplicaPlacementGroup(
        placement_group=mock_pg, _slice_pg=mock_slice_pg
    )

    adapter_with_accel.shutdown()
    mock_slice_pg.shutdown.assert_called_once()
    assert adapter_with_accel._slice_pg is None

    # Call again, should not raise or call shutdown again
    adapter_with_accel.shutdown()
    assert mock_slice_pg.shutdown.call_count == 1


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
