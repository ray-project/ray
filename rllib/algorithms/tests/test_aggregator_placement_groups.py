import unittest

import ray
from ray.rllib.algorithms.impala import IMPALA, IMPALAConfig
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy


@ray.remote(num_cpus=1)
class _AlgoBuilder:
    def __init__(self, config_dict):
        self.algo = IMPALAConfig.from_dict(config_dict).build()

    def inspect_actor_placement(self):
        learner_infos = {}
        for result in self.algo.learner_group.foreach_learner(
            lambda learner: (
                learner._learner_index,
                ray.get_runtime_context().get_placement_group_id(),
                ray.get_runtime_context().get_node_id(),
            )
        ):
            learner_index, placement_group_id, node_id = result.get()
            learner_infos[learner_index] = {
                "placement_group_id": placement_group_id,
                "node_id": node_id,
            }

        aggregator_infos = {}
        for result in self.algo._aggregator_actor_manager.foreach_actor(
            lambda actor: (
                ray.get_runtime_context().get_placement_group_id(),
                ray.get_runtime_context().get_node_id(),
            )
        ):
            placement_group_id, node_id = result.get()
            aggregator_infos[result.actor_id] = {
                "placement_group_id": placement_group_id,
                "node_id": node_id,
            }

        return {
            "builder_placement_group_id": (
                ray.get_runtime_context().get_placement_group_id()
            ),
            "learner_infos": learner_infos,
            "aggregator_infos": aggregator_infos,
            "aggregator_to_learner": dict(self.algo._aggregator_actor_to_learner),
        }

    def stop(self):
        self.algo.stop()


def _get_impala_config_with_aggregators():
    return (
        IMPALAConfig()
        .api_stack(
            enable_env_runner_and_connector_v2=True,
            enable_rl_module_and_learner=True,
        )
        .resources(
            num_cpus_for_main_process=1,
            placement_strategy="PACK",
        )
        .environment("CartPole-v1")
        .env_runners(num_env_runners=0)
        .learners(
            num_learners=1,
            num_cpus_per_learner=1,
            num_gpus_per_learner=0,
            num_aggregator_actors_per_learner=1,
        )
        .framework("torch")
    )


class TestAggregatorPlacementGroups(unittest.TestCase):
    def setUp(self) -> None:
        ray.init(num_cpus=4)

    def tearDown(self) -> None:
        ray.shutdown()

    def _assert_aggregator_context(self, info, expected_pg_id):
        self.assertEqual(len(info["learner_infos"]), 1)
        self.assertEqual(len(info["aggregator_infos"]), 1)

        learner_info = info["learner_infos"][0]
        self.assertEqual(learner_info["placement_group_id"], expected_pg_id)

        for actor_id, aggregator_info in info["aggregator_infos"].items():
            self.assertEqual(aggregator_info["placement_group_id"], expected_pg_id)
            learner_idx = info["aggregator_to_learner"][actor_id]
            self.assertEqual(learner_idx, 0)
            self.assertEqual(
                aggregator_info["node_id"],
                info["learner_infos"][learner_idx]["node_id"],
            )

    def test_impala_aggregators_without_placement_group(self):
        config = _get_impala_config_with_aggregators()
        builder = _AlgoBuilder.remote(config.to_dict())

        try:
            info = ray.get(builder.inspect_actor_placement.remote())
            self.assertIsNone(info["builder_placement_group_id"])
            self._assert_aggregator_context(info, expected_pg_id=None)
        finally:
            ray.get(builder.stop.remote())
            ray.kill(builder)

    def test_impala_aggregators_with_default_placement_group(self):
        config = _get_impala_config_with_aggregators()
        placement_group_factory = IMPALA.default_resource_request(config.to_dict())
        placement_group = placement_group_factory.to_placement_group()
        ray.get(placement_group.ready())

        builder = _AlgoBuilder.options(
            scheduling_strategy=PlacementGroupSchedulingStrategy(
                placement_group=placement_group,
                placement_group_bundle_index=0,
                placement_group_capture_child_tasks=True,
            )
        ).remote(config.to_dict())

        try:
            info = ray.get(builder.inspect_actor_placement.remote())
            self.assertEqual(
                info["builder_placement_group_id"], placement_group.id.hex()
            )
            self._assert_aggregator_context(
                info,
                expected_pg_id=placement_group.id.hex(),
            )
        finally:
            ray.get(builder.stop.remote())
            ray.kill(builder)
            ray.util.remove_placement_group(placement_group)


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
