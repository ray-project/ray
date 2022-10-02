import pytest
import ray
from ray.air import session
from ray.air.execution.impl.tune.tests.common import tune_setup

from ray.air.execution.impl.tune.tune_controller import TuneController
from ray.tune import PlacementGroupFactory, register_trainable
from ray.tune.experiment import Trial
from ray.tune.schedulers import FIFOScheduler, TrialScheduler


def test_change_resources(ray_start_4_cpus_2_gpus):
    """Tests that we can change resources for paused trials.

    Legacy test: test_trial_runner::TrialRunnerTest::testChangeResources

    - Start one trial with 1 CPU and 1 GPU
    - Assert that the allocated resources are correct
        - This is done in our changing resource scheduler
    - Pause trial and update resources
        - Again, this is done in our changing resource scheduler
    - Assert that maximum of parallel running trials is 4
    """

    def _return_cluster_resources(config):
        for i in range(10):
            session.report(
                {"resources": ray.get_runtime_context().get_assigned_resources()}
            )

    register_trainable("_return_cluster_resources", _return_cluster_resources)

    class _ChangeResourcesScheduler(FIFOScheduler):
        def __init__(self):
            super().__init__()
            self._updated = False

        def on_trial_result(
            self, tune_controller: TuneController, trial: Trial, result: dict
        ) -> str:
            if not self._updated:
                # Assert old resources (this result is flattened)
                assert result["resources/CPU"] == 1
                assert result["resources/GPU"] == 1

                self._updated = True
                tune_controller.update_trial_resources(
                    trial, PlacementGroupFactory([{"CPU": 2, "GPU": 2}])
                )
                return TrialScheduler.PAUSE
            return TrialScheduler.CONTINUE

    resource_manager, search_alg, scheduler, trial_states, controller = tune_setup(
        scheduler=_ChangeResourcesScheduler()
    )

    search_alg.add_trial(
        Trial(
            "_return_cluster_resources",
            placement_group_factory=PlacementGroupFactory([{"CPU": 1, "GPU": 1}]),
            stopping_criterion={"training_iteration": 5},
        )
    )

    controller.step_until_finished()

    trial = controller.trials[0]

    assert trial.last_result["resources"]["CPU"] == 2
    assert trial.last_result["resources"]["GPU"] == 2


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
