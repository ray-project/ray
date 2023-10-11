import shutil
import tempfile
import unittest

from ray.tune import PlacementGroupFactory
from ray.tune.execution.tune_controller import TuneController
from ray.tune.schedulers.trial_scheduler import TrialScheduler
from ray.tune.experiment import Trial
from ray.tune.schedulers.resource_changing_scheduler import (
    ResourceChangingScheduler,
    DistributeResources,
    DistributeResourcesToTopJob,
)

from ray.tune.tests.execution.utils import create_execution_test_objects
from ray.train.tests.util import mock_storage_context


class MockTuneController(TuneController):
    def get_live_trials(self):
        return [t for t in self._trials if t.status != "TERMINATED"]


class TestUniformResourceAllocation(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.tune_controller, *_ = create_execution_test_objects(
            resources={"CPU": 8, "GPU": 8},
            reuse_actors=False,
            tune_controller_cls=MockTuneController,
            storage=mock_storage_context(),
        )

    def tearDown(self) -> None:
        shutil.rmtree(self.tmpdir)

    def _prepareTrials(self, scheduler, base_pgf):
        trial1 = Trial("mock", config=dict(num=1), stub=True)
        trial1.placement_group_factory = base_pgf
        trial2 = Trial("mock", config=dict(num=2), stub=True)
        trial2.placement_group_factory = base_pgf
        trial3 = Trial("mock", config=dict(num=3), stub=True)
        trial3.placement_group_factory = base_pgf
        trial4 = Trial("mock", config=dict(num=4), stub=True)
        trial4.placement_group_factory = base_pgf

        self.tune_controller._trials = [trial1, trial2, trial3, trial4]

        scheduler.on_trial_add(self.tune_controller, trial1)
        scheduler.on_trial_add(self.tune_controller, trial2)
        scheduler.on_trial_add(self.tune_controller, trial3)
        scheduler.on_trial_add(self.tune_controller, trial4)

        trial1.status = Trial.RUNNING
        trial2.status = Trial.RUNNING
        trial3.status = Trial.RUNNING
        trial4.status = Trial.RUNNING
        return trial1, trial2, trial3, trial4

    def _allocateAndAssertNewResources(self, trial, scheduler, target_pgf, metric=1):
        result = {"metric": metric, "training_iteration": 4}
        trial.run_metadata.last_result = result
        decision = scheduler.on_trial_result(self.tune_controller, trial, result)
        assert decision == TrialScheduler.PAUSE
        trial.status = Trial.PENDING
        scheduler.choose_trial_to_run(self.tune_controller)
        assert trial.placement_group_factory == target_pgf
        trial.status = Trial.RUNNING

    def testAllocateFreeResources(self):
        scheduler = ResourceChangingScheduler(
            resources_allocation_function=DistributeResources(add_bundles=False)
        )

        base_pgf = PlacementGroupFactory([{"CPU": 1, "GPU": 0}])
        trial1, trial2, trial3, trial4 = self._prepareTrials(scheduler, base_pgf)

        self._allocateAndAssertNewResources(
            trial1, scheduler, PlacementGroupFactory([{"CPU": 2}])
        )
        self._allocateAndAssertNewResources(
            trial2, scheduler, PlacementGroupFactory([{"CPU": 2}])
        )

        trial4.status = Trial.TERMINATED

        self._allocateAndAssertNewResources(
            trial1, scheduler, PlacementGroupFactory([{"CPU": 3}])
        )

        trial3.status = Trial.TERMINATED

        self._allocateAndAssertNewResources(
            trial1, scheduler, PlacementGroupFactory([{"CPU": 4}])
        )

        trial2.status = Trial.TERMINATED

        self._allocateAndAssertNewResources(
            trial1, scheduler, PlacementGroupFactory([{"CPU": 8}])
        )

    def testAllocateFreeResourcesWithIncreaseBy(self):
        scheduler = ResourceChangingScheduler(
            resources_allocation_function=DistributeResources(
                add_bundles=False, increase_by={"CPU": 2, "GPU": 2}
            )
        )

        base_pgf = PlacementGroupFactory([{"CPU": 2, "GPU": 2}])
        trial1, trial2, trial3, trial4 = self._prepareTrials(scheduler, base_pgf)

        decision = scheduler.on_trial_result(
            self.tune_controller, trial1, {"metric": 1, "training_iteration": 4}
        )
        assert decision == TrialScheduler.CONTINUE

        trial4.status = Trial.TERMINATED

        self._allocateAndAssertNewResources(
            trial1, scheduler, PlacementGroupFactory([{"CPU": 4, "GPU": 4}])
        )

        trial3.status = Trial.TERMINATED

        self._allocateAndAssertNewResources(
            trial2, scheduler, PlacementGroupFactory([{"CPU": 4, "GPU": 4}])
        )

        trial2.status = Trial.TERMINATED

        self._allocateAndAssertNewResources(
            trial1, scheduler, PlacementGroupFactory([{"CPU": 8, "GPU": 8}])
        )

    def testAllocateFreeResourcesWithIncreaseByTimes(self):
        scheduler = ResourceChangingScheduler(
            resources_allocation_function=DistributeResources(
                add_bundles=False, increase_by={"GPU": 2}, increase_by_times=2
            )
        )

        base_pgf = PlacementGroupFactory([{"CPU": 1, "GPU": 2}])
        trial1, trial2, trial3, trial4 = self._prepareTrials(scheduler, base_pgf)

        decision = scheduler.on_trial_result(
            self.tune_controller, trial1, {"metric": 1, "training_iteration": 4}
        )
        assert decision == TrialScheduler.CONTINUE

        trial4.status = Trial.TERMINATED

        self._allocateAndAssertNewResources(
            trial1, scheduler, PlacementGroupFactory([{"CPU": 1, "GPU": 4}])
        )

        trial3.status = Trial.TERMINATED

        self._allocateAndAssertNewResources(
            trial2, scheduler, PlacementGroupFactory([{"CPU": 1, "GPU": 4}])
        )

        trial2.status = Trial.TERMINATED

        self._allocateAndAssertNewResources(
            trial1, scheduler, PlacementGroupFactory([{"CPU": 1, "GPU": 6}])
        )

    def testDeallocateResources(self):
        scheduler = ResourceChangingScheduler(
            resources_allocation_function=DistributeResources(
                add_bundles=False, increase_by={"GPU": 2}
            )
        )

        base_pgf = PlacementGroupFactory([{"CPU": 1, "GPU": 2}])
        trial1, trial2, trial3, trial4 = self._prepareTrials(scheduler, base_pgf)
        trial1.placement_group_factory = PlacementGroupFactory([{"CPU": 1, "GPU": 4}])
        trial4.status = Trial.PENDING

        self._allocateAndAssertNewResources(
            trial1, scheduler, PlacementGroupFactory([{"CPU": 1, "GPU": 2}])
        )


class TestUniformResourceAllocationAddBundles(TestUniformResourceAllocation):
    def testAllocateFreeResources(self):
        scheduler = ResourceChangingScheduler(
            resources_allocation_function=DistributeResources(add_bundles=True)
        )

        base_pgf = PlacementGroupFactory([{"CPU": 1, "GPU": 0}])
        trial1, trial2, trial3, trial4 = self._prepareTrials(scheduler, base_pgf)

        self._allocateAndAssertNewResources(
            trial1, scheduler, PlacementGroupFactory([{"CPU": 1}] * 2)
        )
        self._allocateAndAssertNewResources(
            trial2, scheduler, PlacementGroupFactory([{"CPU": 1}] * 2)
        )

        trial4.status = Trial.TERMINATED

        self._allocateAndAssertNewResources(
            trial1, scheduler, PlacementGroupFactory([{"CPU": 1}] * 3)
        )

        trial3.status = Trial.TERMINATED

        self._allocateAndAssertNewResources(
            trial1, scheduler, PlacementGroupFactory([{"CPU": 1}] * 4)
        )

        trial2.status = Trial.TERMINATED

        self._allocateAndAssertNewResources(
            trial1, scheduler, PlacementGroupFactory([{"CPU": 1}] * 8)
        )

    def testAllocateFreeResourcesWithIncreaseBy(self):
        scheduler = ResourceChangingScheduler(
            resources_allocation_function=DistributeResources(
                add_bundles=True, increase_by={"CPU": 2, "GPU": 2}
            )
        )

        base_pgf = PlacementGroupFactory([{}, {"CPU": 2, "GPU": 2}])
        trial1, trial2, trial3, trial4 = self._prepareTrials(scheduler, base_pgf)

        decision = scheduler.on_trial_result(
            self.tune_controller, trial1, {"metric": 1, "training_iteration": 4}
        )
        assert decision == TrialScheduler.CONTINUE

        trial4.status = Trial.TERMINATED

        self._allocateAndAssertNewResources(
            trial1, scheduler, PlacementGroupFactory([{}] + [{"CPU": 2, "GPU": 2}] * 2)
        )

        trial3.status = Trial.TERMINATED

        self._allocateAndAssertNewResources(
            trial2, scheduler, PlacementGroupFactory([{}] + [{"CPU": 2, "GPU": 2}] * 2)
        )

        trial2.status = Trial.TERMINATED

        self._allocateAndAssertNewResources(
            trial1, scheduler, PlacementGroupFactory([{}] + [{"CPU": 2, "GPU": 2}] * 4)
        )

    def testAllocateFreeResourcesWithIncreaseByTimes(self):
        scheduler = ResourceChangingScheduler(
            resources_allocation_function=DistributeResources(
                add_bundles=True, increase_by={"GPU": 2}, increase_by_times=2
            )
        )

        base_pgf = PlacementGroupFactory([{"CPU": 1}, {"GPU": 2}])
        trial1, trial2, trial3, trial4 = self._prepareTrials(scheduler, base_pgf)

        decision = scheduler.on_trial_result(
            self.tune_controller, trial1, {"metric": 1, "training_iteration": 4}
        )
        assert decision == TrialScheduler.CONTINUE

        trial4.status = Trial.TERMINATED

        self._allocateAndAssertNewResources(
            trial1, scheduler, PlacementGroupFactory([{"CPU": 1}] + [{"GPU": 2}] * 2)
        )

        trial3.status = Trial.TERMINATED

        self._allocateAndAssertNewResources(
            trial2, scheduler, PlacementGroupFactory([{"CPU": 1}] + [{"GPU": 2}] * 2)
        )

        trial2.status = Trial.TERMINATED

        self._allocateAndAssertNewResources(
            trial1, scheduler, PlacementGroupFactory([{"CPU": 1}] + [{"GPU": 2}] * 3)
        )

    def testDeallocateResources(self):
        scheduler = ResourceChangingScheduler(
            resources_allocation_function=DistributeResources(
                add_bundles=True, increase_by={"GPU": 2}
            )
        )

        base_pgf = PlacementGroupFactory([{"CPU": 1}, {"GPU": 2}])
        trial1, trial2, trial3, trial4 = self._prepareTrials(scheduler, base_pgf)
        trial1.placement_group_factory = PlacementGroupFactory(
            [{"CPU": 1}] + [{"GPU": 2}] * 2
        )
        trial4.status = Trial.PENDING

        self._allocateAndAssertNewResources(
            trial1, scheduler, PlacementGroupFactory([{"CPU": 1}, {"GPU": 2}])
        )


class TestTopJobResourceAllocation(TestUniformResourceAllocation):
    def _prepareTrials(self, scheduler, base_pgf):
        t1, t2, t3, t4 = super()._prepareTrials(scheduler, base_pgf)
        t1.run_metadata.last_result = {"metric": 1, "training_iteration": 3}
        t2.run_metadata.last_result = {"metric": 0.9, "training_iteration": 3}
        t3.run_metadata.last_result = {"metric": 0.8, "training_iteration": 3}
        t4.run_metadata.last_result = {"metric": 0.7, "training_iteration": 3}
        return t1, t2, t3, t4

    def testAllocateFreeResources(self):
        scheduler = ResourceChangingScheduler(
            resources_allocation_function=DistributeResourcesToTopJob(
                add_bundles=False, metric="metric", mode="max"
            )
        )

        base_pgf = PlacementGroupFactory([{"CPU": 1, "GPU": 0}])
        trial1, trial2, trial3, trial4 = self._prepareTrials(scheduler, base_pgf)

        decision = scheduler.on_trial_result(
            self.tune_controller, trial2, {"metric": 0.9, "training_iteration": 4}
        )
        assert decision == TrialScheduler.CONTINUE

        self._allocateAndAssertNewResources(
            trial1, scheduler, PlacementGroupFactory([{"CPU": 5}])
        )
        decision = scheduler.on_trial_result(
            self.tune_controller, trial2, {"metric": 1.1, "training_iteration": 4}
        )
        assert decision == TrialScheduler.CONTINUE
        trial4.status = Trial.TERMINATED

        self._allocateAndAssertNewResources(
            trial2, scheduler, PlacementGroupFactory([{"CPU": 2}]), metric=1.1
        )
        trial3.status = Trial.TERMINATED

        self._allocateAndAssertNewResources(
            trial1, scheduler, PlacementGroupFactory([{"CPU": 6}]), metric=1.2
        )

        trial2.status = Trial.TERMINATED

        self._allocateAndAssertNewResources(
            trial1, scheduler, PlacementGroupFactory([{"CPU": 8}])
        )

    def testAllocateFreeResourcesWithIncreaseBy(self):
        scheduler = ResourceChangingScheduler(
            resources_allocation_function=DistributeResourcesToTopJob(
                add_bundles=False,
                increase_by={"CPU": 2, "GPU": 2},
                metric="metric",
                mode="max",
            )
        )

        base_pgf = PlacementGroupFactory([{"CPU": 2, "GPU": 2}])
        trial1, trial2, trial3, trial4 = self._prepareTrials(scheduler, base_pgf)

        decision = scheduler.on_trial_result(
            self.tune_controller, trial2, {"metric": 0.9, "training_iteration": 4}
        )
        assert decision == TrialScheduler.CONTINUE

        decision = scheduler.on_trial_result(
            self.tune_controller, trial1, {"metric": 1.0, "training_iteration": 4}
        )
        assert decision == TrialScheduler.CONTINUE

        trial4.status = Trial.TERMINATED

        self._allocateAndAssertNewResources(
            trial1, scheduler, PlacementGroupFactory([{"CPU": 4, "GPU": 4}])
        )
        decision = scheduler.on_trial_result(
            self.tune_controller, trial2, {"metric": 1.1, "training_iteration": 4}
        )
        assert decision == TrialScheduler.CONTINUE
        trial3.status = Trial.TERMINATED

        self._allocateAndAssertNewResources(
            trial2, scheduler, PlacementGroupFactory([{"CPU": 4, "GPU": 4}]), metric=1.1
        )
        trial2.status = Trial.TERMINATED

        self._allocateAndAssertNewResources(
            trial1, scheduler, PlacementGroupFactory([{"CPU": 8, "GPU": 8}]), metric=1.2
        )

    def testAllocateFreeResourcesWithIncreaseByTimes(self):
        scheduler = ResourceChangingScheduler(
            resources_allocation_function=DistributeResourcesToTopJob(
                add_bundles=False,
                increase_by={"GPU": 2},
                increase_by_times=2,
                metric="metric",
                mode="max",
            )
        )

        base_pgf = PlacementGroupFactory([{"CPU": 1, "GPU": 2}])
        trial1, trial2, trial3, trial4 = self._prepareTrials(scheduler, base_pgf)

        decision = scheduler.on_trial_result(
            self.tune_controller, trial2, {"metric": 0.9, "training_iteration": 4}
        )
        assert decision == TrialScheduler.CONTINUE

        decision = scheduler.on_trial_result(
            self.tune_controller, trial1, {"metric": 1.0, "training_iteration": 4}
        )
        assert decision == TrialScheduler.CONTINUE

        trial4.status = Trial.TERMINATED

        self._allocateAndAssertNewResources(
            trial1, scheduler, PlacementGroupFactory([{"CPU": 1, "GPU": 4}])
        )
        decision = scheduler.on_trial_result(
            self.tune_controller, trial2, {"metric": 1.1, "training_iteration": 4}
        )
        assert decision == TrialScheduler.CONTINUE
        trial3.status = Trial.TERMINATED

        self._allocateAndAssertNewResources(
            trial2, scheduler, PlacementGroupFactory([{"CPU": 1, "GPU": 4}]), metric=1.1
        )
        trial2.status = Trial.TERMINATED

        self._allocateAndAssertNewResources(
            trial1, scheduler, PlacementGroupFactory([{"CPU": 1, "GPU": 6}]), metric=1.2
        )

    def testDeallocateResources(self):
        scheduler = ResourceChangingScheduler(
            resources_allocation_function=DistributeResourcesToTopJob(
                add_bundles=False, increase_by={"GPU": 2}, metric="metric", mode="max"
            )
        )

        base_pgf = PlacementGroupFactory([{"CPU": 1, "GPU": 2}])
        trial1, trial2, trial3, trial4 = self._prepareTrials(scheduler, base_pgf)
        trial1.placement_group_factory = PlacementGroupFactory([{"CPU": 1, "GPU": 4}])
        trial4.status = Trial.PENDING

        self._allocateAndAssertNewResources(
            trial1, scheduler, PlacementGroupFactory([{"CPU": 1, "GPU": 2}])
        )


class TestTopJobResourceAllocationAddBundles(TestTopJobResourceAllocation):
    def testAllocateFreeResources(self):
        scheduler = ResourceChangingScheduler(
            resources_allocation_function=DistributeResourcesToTopJob(
                add_bundles=True, metric="metric", mode="max"
            )
        )

        base_pgf = PlacementGroupFactory([{"CPU": 1, "GPU": 0}])
        trial1, trial2, trial3, trial4 = self._prepareTrials(scheduler, base_pgf)

        decision = scheduler.on_trial_result(
            self.tune_controller, trial2, {"metric": 0.9, "training_iteration": 4}
        )
        assert decision == TrialScheduler.CONTINUE

        self._allocateAndAssertNewResources(
            trial1, scheduler, PlacementGroupFactory([{"CPU": 1}] * 5)
        )
        decision = scheduler.on_trial_result(
            self.tune_controller, trial2, {"metric": 1.1, "training_iteration": 4}
        )
        assert decision == TrialScheduler.CONTINUE
        trial4.status = Trial.TERMINATED

        self._allocateAndAssertNewResources(
            trial2, scheduler, PlacementGroupFactory([{"CPU": 1}] * 2), metric=1.1
        )
        trial3.status = Trial.TERMINATED

        self._allocateAndAssertNewResources(
            trial1, scheduler, PlacementGroupFactory([{"CPU": 1}] * 6), metric=1.2
        )

        trial2.status = Trial.TERMINATED

        self._allocateAndAssertNewResources(
            trial1, scheduler, PlacementGroupFactory([{"CPU": 1}] * 8)
        )

    def testAllocateFreeResourcesWithIncreaseBy(self):
        scheduler = ResourceChangingScheduler(
            resources_allocation_function=DistributeResourcesToTopJob(
                add_bundles=True,
                increase_by={"CPU": 2, "GPU": 2},
                metric="metric",
                mode="max",
            )
        )

        base_pgf = PlacementGroupFactory([{}, {"CPU": 2, "GPU": 2}])
        trial1, trial2, trial3, trial4 = self._prepareTrials(scheduler, base_pgf)

        decision = scheduler.on_trial_result(
            self.tune_controller, trial2, {"metric": 0.9, "training_iteration": 4}
        )
        assert decision == TrialScheduler.CONTINUE

        decision = scheduler.on_trial_result(
            self.tune_controller, trial1, {"metric": 1.0, "training_iteration": 4}
        )
        assert decision == TrialScheduler.CONTINUE

        trial4.status = Trial.TERMINATED

        self._allocateAndAssertNewResources(
            trial1, scheduler, PlacementGroupFactory([{}] + [{"CPU": 2, "GPU": 2}] * 2)
        )
        decision = scheduler.on_trial_result(
            self.tune_controller, trial2, {"metric": 1.1, "training_iteration": 4}
        )
        assert decision == TrialScheduler.CONTINUE
        trial3.status = Trial.TERMINATED

        self._allocateAndAssertNewResources(
            trial2,
            scheduler,
            PlacementGroupFactory([{}] + [{"CPU": 2, "GPU": 2}] * 2),
            metric=1.1,
        )
        trial2.status = Trial.TERMINATED

        self._allocateAndAssertNewResources(
            trial1,
            scheduler,
            PlacementGroupFactory([{}] + [{"CPU": 2, "GPU": 2}] * 4),
            metric=1.2,
        )

    def testAllocateFreeResourcesWithIncreaseByTimes(self):
        scheduler = ResourceChangingScheduler(
            resources_allocation_function=DistributeResourcesToTopJob(
                add_bundles=True,
                increase_by={"GPU": 2},
                increase_by_times=2,
                metric="metric",
                mode="max",
            )
        )

        base_pgf = PlacementGroupFactory([{"CPU": 1}, {"GPU": 2}])
        trial1, trial2, trial3, trial4 = self._prepareTrials(scheduler, base_pgf)

        decision = scheduler.on_trial_result(
            self.tune_controller, trial2, {"metric": 0.9, "training_iteration": 4}
        )
        assert decision == TrialScheduler.CONTINUE

        decision = scheduler.on_trial_result(
            self.tune_controller, trial1, {"metric": 1.0, "training_iteration": 4}
        )
        assert decision == TrialScheduler.CONTINUE

        trial4.status = Trial.TERMINATED

        self._allocateAndAssertNewResources(
            trial1, scheduler, PlacementGroupFactory([{"CPU": 1}] + [{"GPU": 2}] * 2)
        )
        decision = scheduler.on_trial_result(
            self.tune_controller, trial2, {"metric": 1.1, "training_iteration": 4}
        )
        assert decision == TrialScheduler.CONTINUE
        trial3.status = Trial.TERMINATED

        self._allocateAndAssertNewResources(
            trial2,
            scheduler,
            PlacementGroupFactory([{"CPU": 1}] + [{"GPU": 2}] * 2),
            metric=1.1,
        )
        trial2.status = Trial.TERMINATED

        self._allocateAndAssertNewResources(
            trial1,
            scheduler,
            PlacementGroupFactory([{"CPU": 1}] + [{"GPU": 2}] * 3),
            metric=1.2,
        )

    def testDeallocateResources(self):
        scheduler = ResourceChangingScheduler(
            resources_allocation_function=DistributeResourcesToTopJob(
                add_bundles=True, increase_by={"GPU": 2}, metric="metric", mode="max"
            )
        )

        base_pgf = PlacementGroupFactory([{"CPU": 1}, {"GPU": 2}])
        trial1, trial2, trial3, trial4 = self._prepareTrials(scheduler, base_pgf)
        trial1.placement_group_factory = PlacementGroupFactory(
            [{"CPU": 1}] + [{"GPU": 2}] * 2
        )
        trial4.status = Trial.PENDING

        self._allocateAndAssertNewResources(
            trial1, scheduler, PlacementGroupFactory([{"CPU": 1}, {"GPU": 2}])
        )


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
