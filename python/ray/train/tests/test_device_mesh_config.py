import pytest

from ray.train._internal.device_mesh_config import (
    DeviceMeshConfig,
    DPConfig,
    get_data_shard_mapping,
)


class TestDPConfigPostInit:
    """Tests for DPConfig.__post_init__"""

    @pytest.mark.parametrize(
        "replicate,shard",
        [
            (1, 1),  # default/minimum values
            (4, 8),  # explicit values
            ("auto", 4),  # auto replicate
            (2, 1),  # boundary: shard=1
        ],
    )
    def test_valid_configs(self, replicate, shard):
        """Test DPConfig with valid configurations."""
        config = DPConfig(replicate=replicate, shard=shard)
        assert config.replicate == replicate
        assert config.shard == shard

    @pytest.mark.parametrize(
        "replicate,shard,error_match",
        [
            (0, 1, "replicate must be >= 1 or 'auto'"),
            (-1, 1, "replicate must be >= 1 or 'auto'"),
            (1, 0, "shard must be >= 1"),
            (1, -2, "shard must be >= 1"),
        ],
    )
    def test_invalid_configs(self, replicate, shard, error_match):
        """Test DPConfig raises ValueError for invalid configurations."""
        with pytest.raises(ValueError, match=error_match):
            DPConfig(replicate=replicate, shard=shard)


class TestDeviceMeshConfigPostInit:
    """Tests for DeviceMeshConfig.__post_init__"""

    def test_default_values(self):
        """Test DeviceMeshConfig with default values."""
        config = DeviceMeshConfig()
        assert config.tp == 1
        assert config.pp == 1
        assert config.cp == 1
        assert config.ep == 1
        assert config.dp.replicate == 1
        assert config.dp.shard == 1
        assert config.axis_order == ("pp", "dp_rep", "dp_shard", "ep", "cp", "tp")

    def test_valid_parallel_dimensions(self):
        """Test DeviceMeshConfig with valid parallel dimension values."""
        config = DeviceMeshConfig(tp=4, pp=2, cp=2, ep=2)
        assert config.tp == 4
        assert config.pp == 2
        assert config.cp == 2
        assert config.ep == 2

    @pytest.mark.parametrize(
        "axis_order",
        [
            ("tp", "cp", "ep", "dp_shard", "dp_rep", "pp"),  # custom order
            ("tp", "dp_rep"),  # partial order
        ],
    )
    def test_valid_axis_order(self, axis_order):
        """Test DeviceMeshConfig with valid axis orders."""
        config = DeviceMeshConfig(axis_order=axis_order)
        assert config.axis_order == axis_order

    @pytest.mark.parametrize(
        "dim,value",
        [
            ("tp", 0),
            ("tp", -1),
            ("pp", 0),
            ("pp", -1),
            ("cp", 0),
            ("cp", -1),
            ("ep", 0),
            ("ep", -1),
        ],
    )
    def test_invalid_dimension_values(self, dim, value):
        """Test DeviceMeshConfig raises ValueError for invalid dimension values."""
        with pytest.raises(ValueError, match=f"{dim} must be >= 1"):
            DeviceMeshConfig(**{dim: value})

    @pytest.mark.parametrize(
        "axis_order,error_match",
        [
            (("tp", "invalid_axis", "pp"), "Invalid axis 'invalid_axis'"),
            (("tp", "tp", "pp"), "Duplicate axes"),
            (("tp", "tp", "pp", "pp"), "Duplicate axes"),
        ],
    )
    def test_invalid_axis_order(self, axis_order, error_match):
        """Test DeviceMeshConfig raises ValueError for invalid axis orders."""
        with pytest.raises(ValueError, match=error_match):
            DeviceMeshConfig(axis_order=axis_order)


class TestDeviceMeshConfigFixedDimsProduct:
    """Tests for DeviceMeshConfig._fixed_dims_product"""

    @pytest.mark.parametrize(
        "config_kwargs,expected",
        [
            ({}, 1),  # default values
            ({"tp": 4}, 4),
            ({"pp": 2}, 2),
            ({"cp": 3}, 3),
            ({"ep": 8}, 8),
            ({"dp": DPConfig(shard=4)}, 4),
            ({"dp": DPConfig(replicate=8, shard=1)}, 1),  # replicate not included
            (
                {
                    "tp": 4,
                    "pp": 2,
                    "cp": 2,
                    "ep": 2,
                    "dp": DPConfig(replicate=2, shard=2),
                },
                64,
            ),
        ],
    )
    def test_fixed_dims_product(self, config_kwargs, expected):
        """Test _fixed_dims_product computation."""
        config = DeviceMeshConfig(**config_kwargs)
        assert config._fixed_dims_product == expected

    def test_cached_property(self):
        """Test that _fixed_dims_product is cached."""
        config = DeviceMeshConfig(tp=4, pp=2)
        result1 = config._fixed_dims_product
        result2 = config._fixed_dims_product
        assert result1 == result2 == 8


class TestDeviceMeshConfigValidateWorldSize:
    """Tests for DeviceMeshConfig.validate_world_size"""

    @pytest.mark.parametrize(
        "config_kwargs,world_size",
        [
            ({}, 1),  # default config
            ({"tp": 4, "pp": 2, "dp": DPConfig(replicate=2, shard=1)}, 16),
            ({"tp": 4, "pp": 2, "dp": DPConfig(replicate="auto", shard=1)}, 16),
            (
                {
                    "tp": 2,
                    "pp": 2,
                    "cp": 2,
                    "ep": 2,
                    "dp": DPConfig(replicate=2, shard=2),
                },
                64,
            ),
        ],
    )
    def test_valid_world_size(self, config_kwargs, world_size):
        """Test valid world sizes."""
        config = DeviceMeshConfig(**config_kwargs)
        config.validate_world_size(world_size)  # Should not raise

    @pytest.mark.parametrize(
        "config_kwargs,world_size,error_match",
        [
            ({"tp": 4, "pp": 2, "dp": DPConfig(replicate=2, shard=1)}, 8, "must equal"),
            (
                {"tp": 4, "pp": 2, "dp": DPConfig(replicate="auto", shard=1)},
                10,
                "must be divisible",
            ),
            ({"tp": 8, "dp": DPConfig(replicate=2, shard=1)}, 4, "must equal"),
        ],
    )
    def test_invalid_world_size(self, config_kwargs, world_size, error_match):
        """Test invalid world sizes."""
        config = DeviceMeshConfig(**config_kwargs)
        with pytest.raises(ValueError, match=error_match):
            config.validate_world_size(world_size)


class TestDeviceMeshConfigGetDPReplicate:
    """Tests for DeviceMeshConfig.get_dp_replicate"""

    @pytest.mark.parametrize(
        "config_kwargs,world_size,expected",
        [
            # Explicit replicate
            ({"dp": DPConfig(replicate=4, shard=1)}, 4, 4),
            ({"dp": DPConfig(replicate=4, shard=1)}, 100, 4),  # ignores world_size
            # Auto replicate
            ({"dp": DPConfig(replicate="auto", shard=1)}, 8, 8),
            ({"tp": 4, "dp": DPConfig(replicate="auto", shard=1)}, 16, 4),
            (
                {"tp": 2, "pp": 2, "cp": 2, "dp": DPConfig(replicate="auto", shard=2)},
                64,
                4,
            ),
            ({"tp": 8, "dp": DPConfig(replicate="auto", shard=1)}, 8, 1),
        ],
    )
    def test_get_dp_replicate(self, config_kwargs, world_size, expected):
        """Test get_dp_replicate returns correct value."""
        config = DeviceMeshConfig(**config_kwargs)
        assert config.get_dp_replicate(world_size=world_size) == expected


class TestDeviceMeshConfigGetAxisSize:
    """Tests for DeviceMeshConfig.get_axis_size"""

    @pytest.mark.parametrize(
        "config_kwargs,axis,world_size,expected",
        [
            ({"tp": 8}, "tp", 8, 8),
            ({"pp": 4}, "pp", 4, 4),
            ({"cp": 2}, "cp", 2, 2),
            ({"ep": 16}, "ep", 16, 16),
            ({"dp": DPConfig(replicate=1, shard=4)}, "dp_shard", 4, 4),
            ({"dp": DPConfig(replicate=4, shard=1)}, "dp_rep", 4, 4),
            ({"tp": 4, "dp": DPConfig(replicate="auto", shard=1)}, "dp_rep", 16, 4),
        ],
    )
    def test_get_axis_size(self, config_kwargs, axis, world_size, expected):
        """Test get_axis_size returns correct value for each axis."""
        config = DeviceMeshConfig(**config_kwargs)
        assert config.get_axis_size(axis, world_size=world_size) == expected

    def test_unknown_axis_raises_error(self):
        """Test get_axis_size raises ValueError for unknown axis."""
        config = DeviceMeshConfig()
        with pytest.raises(ValueError, match="Unknown axis"):
            config.get_axis_size("unknown_axis", world_size=1)

    def test_all_axes_in_complex_config(self):
        """Test get_axis_size for all axes in a complex configuration."""
        config = DeviceMeshConfig(
            tp=4, pp=2, cp=3, ep=2, dp=DPConfig(replicate=2, shard=4)
        )
        world_size = 4 * 2 * 3 * 2 * 2 * 4  # 384

        assert config.get_axis_size("tp", world_size) == 4
        assert config.get_axis_size("pp", world_size) == 2
        assert config.get_axis_size("cp", world_size) == 3
        assert config.get_axis_size("ep", world_size) == 2
        assert config.get_axis_size("dp_rep", world_size) == 2
        assert config.get_axis_size("dp_shard", world_size) == 4


class TestGetDataShardMapping:
    """Tests for get_data_shard_mapping function"""

    def test_single_worker(self):
        """Test mapping with single worker."""
        config = DeviceMeshConfig()
        mapping = get_data_shard_mapping(config, world_size=1)
        assert mapping == {0: (0, 0)}

    @pytest.mark.parametrize(
        "dp_config,world_size,expected_num_shards",
        [
            (DPConfig(replicate=4, shard=1), 4, 4),  # pure replicate
            (DPConfig(replicate=1, shard=4), 4, 4),  # pure shard (FSDP)
            (DPConfig(replicate=2, shard=2), 4, 4),  # HSDP
        ],
    )
    def test_pure_dp_configs(self, dp_config, world_size, expected_num_shards):
        """Test mapping with various pure DP configurations."""
        config = DeviceMeshConfig(dp=dp_config)
        mapping = get_data_shard_mapping(config, world_size=world_size)

        assert len(mapping) == world_size
        data_shard_ids = {mapping[rank][0] for rank in range(world_size)}
        assert len(data_shard_ids) == expected_num_shards

    def test_tp_only(self):
        """Test mapping with tensor parallelism only."""
        config = DeviceMeshConfig(tp=4)
        mapping = get_data_shard_mapping(config, world_size=4)

        # All ranks share the same data (data_shard_id=0)
        assert len(mapping) == 4
        for rank in range(4):
            data_shard_id, rank_in_shard = mapping[rank]
            assert data_shard_id == 0
            assert rank_in_shard == rank

    @pytest.mark.parametrize(
        "config_kwargs,world_size,expected_num_shards,expected_shard_size",
        [
            ({"tp": 4, "dp": DPConfig(replicate=2, shard=1)}, 8, 2, 4),
            ({"pp": 2, "tp": 4, "dp": DPConfig(replicate=2, shard=1)}, 16, 2, 8),
            ({"tp": 4, "dp": DPConfig(replicate="auto", shard=1)}, 16, 4, 4),
        ],
    )
    def test_mixed_parallelism(
        self, config_kwargs, world_size, expected_num_shards, expected_shard_size
    ):
        """Test mapping with mixed parallelism configurations."""
        config = DeviceMeshConfig(**config_kwargs)
        mapping = get_data_shard_mapping(config, world_size=world_size)

        assert len(mapping) == world_size

        shard_counts = {}
        for rank in range(world_size):
            shard_id = mapping[rank][0]
            shard_counts[shard_id] = shard_counts.get(shard_id, 0) + 1

        assert len(shard_counts) == expected_num_shards
        for count in shard_counts.values():
            assert count == expected_shard_size

    def test_custom_axis_order(self):
        """Test mapping with custom axis order."""
        config = DeviceMeshConfig(
            tp=2,
            dp=DPConfig(replicate=2, shard=1),
            axis_order=("dp_rep", "tp"),  # DP outer, TP inner
        )
        mapping = get_data_shard_mapping(config, world_size=4)

        assert len(mapping) == 4
        # With this order: rank 0,1 are in shard 0, rank 2,3 are in shard 1
        assert mapping[0][0] == 0
        assert mapping[1][0] == 0
        assert mapping[2][0] == 1
        assert mapping[3][0] == 1

    def test_all_ranks_have_unique_combination(self):
        """Test that all ranks have unique (data_shard_id, rank_in_shard) pairs."""
        config = DeviceMeshConfig(tp=2, pp=2, dp=DPConfig(replicate=2, shard=2))
        mapping = get_data_shard_mapping(config, world_size=16)

        combinations = list(mapping.values())
        assert len(combinations) == len(set(combinations))

    def test_invalid_world_size_raises_error(self):
        """Test that invalid world size raises ValueError."""
        config = DeviceMeshConfig(tp=4, dp=DPConfig(replicate=2, shard=1))
        with pytest.raises(ValueError):
            get_data_shard_mapping(config, world_size=10)

    def test_complex_config_consistency(self):
        """Test consistency in a complex configuration."""
        config = DeviceMeshConfig(
            tp=2, pp=2, cp=1, ep=2, dp=DPConfig(replicate=2, shard=2)
        )
        world_size = 2 * 2 * 1 * 2 * 2 * 2  # 32

        mapping = get_data_shard_mapping(config, world_size=world_size)

        assert len(mapping) == world_size

        num_data_shards = 2 * 2  # dp_rep * dp_shard
        shard_group_size = 2 * 2 * 1 * 2  # tp * pp * cp * ep

        shard_counts = {}
        for rank in range(world_size):
            shard_id = mapping[rank][0]
            shard_counts[shard_id] = shard_counts.get(shard_id, 0) + 1

        assert len(shard_counts) == num_data_shards
        for count in shard_counts.values():
            assert count == shard_group_size


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
