import unittest

import pytest

import ray
import ray.rllib.algorithms.impala as impala
from ray.cluster_utils import Cluster
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.utils.metrics import LEARNER_RESULTS
from ray.rllib.utils.test_utils import check


class TestIMPALA(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_impala_minibatch_size_check(self):
        config = (
            impala.IMPALAConfig()
            .environment("CartPole-v1")
            .training(minibatch_size=100)
            .env_runners(rollout_fragment_length=30)
        )

        with pytest.raises(
            ValueError,
            match=r"`minibatch_size` \(100\) must either be None or a multiple of `rollout_fragment_length` \(30\)",
        ):
            config.validate()

    def test_impala_lr_schedule(self):
        # Test whether we correctly ignore the "lr" setting.
        # The first lr should be 0.05.
        config = (
            impala.IMPALAConfig()
            .learners(num_learners=0)
            .experimental(_validate_config=False)  #
            .training(
                lr=[
                    [0, 0.05],
                    [100000, 0.000001],
                ],
                train_batch_size=100,
            )
            .env_runners(num_envs_per_env_runner=2)
            .environment(env="CartPole-v1")
        )

        def get_lr(result):
            return result[LEARNER_RESULTS][DEFAULT_POLICY_ID][
                "default_optimizer_learning_rate"
            ]

        algo = config.build()
        optim = algo.learner_group._learner.get_optimizer()

        try:
            check(optim.param_groups[0]["lr"], 0.05)
            for _ in range(1):
                r1 = algo.train()
            for _ in range(2):
                r2 = algo.train()
            for _ in range(2):
                r3 = algo.train()
            # Due to the asynch'ness of IMPALA, learner-stats metrics
            # could be delayed by one iteration. Do 3 train() calls here
            # and measure guaranteed decrease in lr between 1st and 3rd.
            lr1 = get_lr(r1)
            lr2 = get_lr(r2)
            lr3 = get_lr(r3)
            assert lr2 <= lr1, (lr1, lr2)
            assert lr3 <= lr2, (lr2, lr3)
            assert lr3 < lr1, (lr1, lr3)
        finally:
            algo.stop()

    def test_aggregator_actors_colocate_with_learners(self):
        """Verifies PG-bundle-based aggregator placement.

        With one node, colocation is trivially true, so we spin up a fake
        2-node cluster, force Learners onto distinct nodes by giving each
        node 1 unit of a custom ``learner_slot`` resource and asking each
        Learner for 1 unit, then check that each AggregatorActor lands on
        the same node as its assigned Learner. Aggregators share their
        learner's placement-group bundle, so the colocation falls out of
        the PG bundle's node assignment.
        """
        # Shutdown ray first to make sure we have a clean cluster to work with.
        ray.shutdown()

        # Each node advertises 1 unit of `learner_slot`. With 2 Learners
        # each requesting 1 unit (via `custom_resources_per_learner`), Ray
        # *must* place them on different nodes -- the only way both can
        # claim the resource.
        cluster = Cluster(
            initialize_head=True,
            head_node_args={"num_cpus": 4, "resources": {"learner_slot": 1}},
        )
        cluster.add_node(num_cpus=4, resources={"learner_slot": 1})
        cluster.wait_for_nodes()
        ray.init(address=cluster.address)

        config = (
            impala.IMPALAConfig()
            .environment("CartPole-v1")
            .learners(
                num_learners=2,
                num_aggregator_actors_per_learner=2,
                custom_resources_per_learner={"learner_slot": 1},
            )
            .env_runners(num_env_runners=0)
        )
        algo = config.build()

        mgr = algo._aggregator_actor_manager
        self.assertIsNotNone(mgr)
        self.assertEqual(len(mgr._actors), 4)

        mapping = algo._aggregator_actor_to_learner
        self.assertEqual(set(mapping.values()), {0, 1})
        # Each Learner gets exactly num_aggregator_actors_per_learner
        # aggregators.
        self.assertEqual(sorted(mapping.values()), [0, 0, 1, 1])

        learner_node_ids = [
            rc.get()
            for rc in algo.learner_group.foreach_learner(
                func=lambda _l: ray.get_runtime_context().get_node_id()
            )
        ]
        # Sanity check: with 2 nodes and 2 Learners, learners must land
        # on distinct nodes for the affinity assertion below to be
        # meaningful. If they don't, the spread config is broken.
        self.assertEqual(len(set(learner_node_ids)), 2)

        agg_node_ids = [
            rc.get()
            for rc in mgr.foreach_actor(
                func=lambda _a: ray.get_runtime_context().get_node_id()
            )
        ]
        for agg_idx, agg_node in enumerate(agg_node_ids):
            expected = learner_node_ids[mapping[agg_idx]]
            self.assertEqual(agg_node, expected)

        # Disconnect the driver BEFORE tearing down the cluster — otherwise
        # `cluster.shutdown()` raises `ValueError: Removing a node that is
        # connected to this Ray client is not allowed`.
        ray.shutdown()
        cluster.shutdown()

    def test_local_learner_thread_stops_on_algo_stop(self):
        # Regression test: `algo.stop()` -> `LearnerGroup.shutdown()` ->
        # `IMPALALearner.shutdown()` must stop and join the local IMPALA
        # `_LearnerThread`. Otherwise the daemon thread keeps spinning and
        # can race against interpreter shutdown inside an auto_init-wrapped
        # Ray API.
        config = (
            impala.IMPALAConfig()
            .environment("CartPole-v1")
            .learners(num_learners=0)
            .env_runners(num_env_runners=0)
        )
        algo = config.build()
        learner_thread = algo.learner_group._learner._learner_thread
        self.assertTrue(learner_thread.is_alive())

        algo.stop()

        # `Learner.shutdown()` joins the thread, so it must be dead by the
        # time `algo.stop()` returns — no extra `join()` needed here.
        self.assertFalse(learner_thread.is_alive())


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
