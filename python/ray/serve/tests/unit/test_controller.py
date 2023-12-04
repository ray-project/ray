from copy import deepcopy

import pytest

from ray.serve._private.common import TargetCapacityDirection
from ray.serve._private.controller import (
    applications_match,
    calculate_target_capacity_direction,
)
from ray.serve.schema import (
    HTTPOptionsSchema,
    ServeApplicationSchema,
    ServeDeploySchema,
)


def create_app_config(name: str) -> ServeApplicationSchema:
    return ServeApplicationSchema(
        name=name, import_path=f"fake.{name}", route_prefix=f"/{name}"
    )


class TestApplicationsMatch:
    def test_config_with_self(self):
        config = ServeDeploySchema(
            applications=[
                ServeApplicationSchema(
                    name="app1",
                    import_path="fake.import",
                    route_prefix="/",
                ),
            ],
        )

        assert applications_match(config, config) is True

    def test_configs_with_matching_app_names(self):
        config1 = ServeDeploySchema(
            applications=[
                ServeApplicationSchema(
                    name="app1",
                    import_path="fake.import",
                    route_prefix="/app1",
                ),
                ServeApplicationSchema(
                    name="app2",
                    import_path="fake.import2",
                    route_prefix="/app2",
                ),
                ServeApplicationSchema(
                    name="app3",
                    import_path="fake.import3",
                    route_prefix="/app3",
                ),
            ],
        )

        # Configs contain apps with same name but different import paths.
        config2 = deepcopy(config1)
        config2.applications[0].import_path = "different_fake.import"
        assert applications_match(config1, config2) is True

        config2.applications[0].import_path = "extended.fake.import"
        assert applications_match(config1, config2) is True

        config2.applications[0].import_path = "fake:import"
        assert applications_match(config1, config2) is True

        # Configs contain apps with same name but different route_prefixes.
        config2 = deepcopy(config1)
        config2.applications[0].route_prefix += "_suffix"
        assert applications_match(config1, config2) is True

        # Configs contain apps with same name but different runtime_envs.
        config2 = deepcopy(config1)
        config2.applications[1].runtime_env = {"working_dir": "https://fake/uri"}
        assert applications_match(config1, config2) is True

        # Configs contain apps with same name but different target_capacities.
        config2 = deepcopy(config1)
        config2.target_capacity = 50
        assert applications_match(config1, config2) is True

        # Configs contain apps with same name but different http options.
        config2 = deepcopy(config1)
        config2.http_options = HTTPOptionsSchema(host="62.79.45.100")
        assert applications_match(config1, config2) is True

    def test_configs_with_different_app_names(self):
        config1 = ServeDeploySchema(
            applications=[
                ServeApplicationSchema(
                    name="app1",
                    import_path="fake.import",
                    route_prefix="/app1",
                ),
                ServeApplicationSchema(
                    name="app2",
                    import_path="fake.import2",
                    route_prefix="/app2",
                ),
                ServeApplicationSchema(
                    name="app3",
                    import_path="fake.import3",
                    route_prefix="/app3",
                ),
            ],
        )

        # Configs contain apps with different names but same import paths.
        config2 = deepcopy(config1)
        config2.applications[0].name = "different_app1"
        assert applications_match(config1, config2) is False

        # Configs contain different number of apps.
        config2 = deepcopy(config1)
        config2.applications.pop()
        assert applications_match(config1, config2) is False


class TestCalculateScaleDirection:
    @pytest.mark.parametrize(
        "curr_direction",
        [TargetCapacityDirection.UP, TargetCapacityDirection.DOWN],
    )
    @pytest.mark.parametrize(
        "new_direction",
        [TargetCapacityDirection.UP, TargetCapacityDirection.DOWN],
    )
    def test_change_target_capacity_numeric(self, curr_direction, new_direction):
        curr_target_capacity = 5

        curr_config = ServeDeploySchema(
            target_capacity=curr_target_capacity,
            applications=[
                create_app_config(name="app1"),
                create_app_config(name="app2"),
            ],
        )

        if new_direction == TargetCapacityDirection.UP:
            new_target_capacity = curr_target_capacity * 3.3
        elif new_direction == TargetCapacityDirection.DOWN:
            new_target_capacity = curr_target_capacity / 3.3

        new_config = deepcopy(curr_config)
        new_config.target_capacity = new_target_capacity

        # The new direction must be returned, regardless of the current direction.
        assert (
            calculate_target_capacity_direction(
                curr_config,
                new_config,
                curr_direction,
            )
            == new_direction
        )

    @pytest.mark.parametrize("target_capacity", [0, 50, 100, None])
    @pytest.mark.parametrize(
        "curr_direction",
        [TargetCapacityDirection.UP, TargetCapacityDirection.DOWN, None],
    )
    def test_no_change_target_capacity(self, target_capacity, curr_direction):
        """When target_capacity doesn't change, return the current direction."""

        if target_capacity is None:
            # When target_capacity is None, the current direction must be None.
            curr_direction = None

        curr_config = ServeDeploySchema(
            target_capacity=target_capacity,
            applications=[
                create_app_config(name="app1"),
                create_app_config(name="app2"),
                create_app_config(name="app3"),
            ],
        )

        assert (
            calculate_target_capacity_direction(
                curr_config,
                curr_config,
                curr_direction,
            )
            == curr_direction
        )

    @pytest.mark.parametrize("curr_target_capacity", [0, 50, 100, None])
    @pytest.mark.parametrize(
        "curr_direction",
        [TargetCapacityDirection.UP, TargetCapacityDirection.DOWN, None],
    )
    def test_enter_null_target_capacity(self, curr_target_capacity, curr_direction):
        """When target capacity becomes null, scale up/down behavior must stop."""

        if curr_target_capacity is None:
            # When target_capacity is None, the current direction must be None.
            curr_direction = None

        curr_config = ServeDeploySchema(
            target_capacity=curr_target_capacity,
            applications=[
                create_app_config(name="app1"),
                create_app_config(name="app2"),
                create_app_config(name="app3"),
            ],
        )

        new_config = deepcopy(curr_config)
        new_config.target_capacity = None

        assert (
            calculate_target_capacity_direction(
                curr_config,
                new_config,
                curr_direction,
            )
            is None
        )

    @pytest.mark.parametrize("new_target_capacity", [0, 50, 100])
    def test_exit_null_target_capacity(self, new_target_capacity):
        """When target capacity goes null -> non-null, scale up must start."""

        curr_config = ServeDeploySchema(
            target_capacity=None,
            applications=[create_app_config(name="app1")],
        )

        new_config = deepcopy(curr_config)
        new_config.target_capacity = new_target_capacity

        # When Serve is already running the applications at target_capacity
        # None, and then a target_capacity is applied, the direction must
        # become DOWN.
        assert (
            calculate_target_capacity_direction(
                curr_config,
                new_config,
                None,
            )
            == TargetCapacityDirection.UP
        )

    def test_scale_up_first_config(self):
        """Check how Serve handles the first config that's applied."""

        # Case 1: target_capacity is set. Serve should transition to scaling up.

        new_config = ServeDeploySchema(
            target_capacity=20,
            applications=[create_app_config(name="app1")],
        )
        assert (
            calculate_target_capacity_direction(
                None,
                new_config,
                None,
            )
            == TargetCapacityDirection.UP
        )

        # Case 2: target_capacity is not set. Serve should not be scaling.

        new_config = ServeDeploySchema(
            target_capacity=None,
            applications=[create_app_config(name="app1")],
        )
        assert (
            calculate_target_capacity_direction(
                None,
                new_config,
                None,
            )
            is None
        )

    @pytest.mark.parametrize("curr_target_capacity", [0, 50, 100, None])
    @pytest.mark.parametrize(
        "curr_direction",
        [TargetCapacityDirection.UP, TargetCapacityDirection.DOWN, None],
    )
    def test_config_live_apps_mismatch(self, curr_target_capacity, curr_direction):
        """Apply a config with apps that don't match the live apps.

        Serve should treat this like applying the first config. Its scaling
        direction should not be based on the previous config's target_capacity.
        """

        if curr_target_capacity is None:
            # When target_capacity is None, the current direction must be None.
            curr_direction = None

        curr_config = ServeDeploySchema(
            target_capacity=curr_target_capacity,
            applications=[
                create_app_config(name="app1"),
                create_app_config(name="app2"),
                create_app_config(name="app3"),
            ],
        )

        # Case 1: target_capacity is set. Serve should transition to scaling up.
        new_config = ServeDeploySchema(
            target_capacity=30,
            applications=[
                create_app_config(name="new_app1"),
                create_app_config(name="app2"),
            ],
        )

        assert (
            calculate_target_capacity_direction(
                curr_config,
                new_config,
                curr_direction,
            )
            is TargetCapacityDirection.UP
        )

        # Case 2: target_capacity is not set. Serve should not be scaling.
        new_config = ServeDeploySchema(
            target_capacity=None,
            applications=[
                create_app_config(name="new_app1"),
                create_app_config(name="app2"),
            ],
        )

        assert (
            calculate_target_capacity_direction(
                curr_config,
                new_config,
                curr_direction,
            )
            is None
        )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
