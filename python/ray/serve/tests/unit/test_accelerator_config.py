import sys
from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError

from ray.serve._private.common import CreatePlacementGroupRequest
from ray.serve._private.default_impl import (
    ReplicaPlacementGroup,
    _create_replica_placement_group,
    _default_create_placement_group,
)
from ray.serve.api import deployment
from ray.serve.config import TPUAcceleratorConfig
from ray.util.placement_group import PlacementGroup


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


@pytest.mark.parametrize(
    "creation_fn, expects_wrapper",
    [
        (_default_create_placement_group, False),
        (_create_replica_placement_group, True),
    ],
)
def test_placement_group_creation_types(creation_fn, expects_wrapper):
    """Verify that external overrides return bare PGs while internal ones return wrappers."""
    request = CreatePlacementGroupRequest(
        bundles=[{"CPU": 1.0}],
        strategy="SPREAD",
        target_node_id="",
        name="test",
    )

    mock_pg = MagicMock(spec=PlacementGroup)
    with patch("ray.util.placement_group", return_value=mock_pg):
        result = creation_fn(request)

    if expects_wrapper:
        assert isinstance(result, ReplicaPlacementGroup)
        assert result.placement_group == mock_pg
    else:
        assert result == mock_pg
        assert not isinstance(result, ReplicaPlacementGroup)


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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
