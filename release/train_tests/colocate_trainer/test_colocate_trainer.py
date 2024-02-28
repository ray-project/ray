"""Ray Train release test: Colocate Trainer and Rank 0 worker

Setup:
- 1 x g4dn.4xlarge (16 CPU, 1 GPU, 64 GB Memory)
- 3 x g4dn.xlarge (4 CPU, 1 GPU, 16 GB memory)

Test owner: woshiyyya
"""

from ray.train.data_parallel_trainer import DataParallelTrainer
from ray.train.backend import Backend, BackendConfig
from ray.train import RunConfig, ScalingConfig

@pytest.mark.parametrize("trainer_resources", [None, {"memory": 40 * 1024 ** 3}, {"CPU": 10}])
@pytest.mark.parametrize(
    "resources_per_worker_and_use_gpu",
    [
        (None, True),
        ({"CPU": 1}, False),
        ({"GPU": 1}, True),
    ],
)
def test_colocate_trainer_and_rank0_worker(
    ray_start_heterogenous_cluster,
    trainer_resources,
    resources_per_worker_and_use_gpu,
):
    def train_func():
        pass

    class CustomBackend(Backend):
        def on_training_start(self, worker_group, backend_config):
            trainer_node_id = ray.get_runtime_context().get_node_id()

            def check_node_id():
                if ray.train.get_context().get_world_rank() == 0:
                    assert trainer_node_id == ray.get_runtime_context().get_node_id()

            worker_group.execute(check_node_id)

    class CustomBackendConfig(BackendConfig):
        @property
        def backend_cls(self):
            return CustomBackend

    for num_workers in [1, 2, 4]:
        scale_config = ScalingConfig(
            num_workers=num_workers,
            use_gpu=use_gpu,
            trainer_resources=trainer_resources,
            resources_per_worker=resources_per_worker,
        )

        trainer = DataParallelTrainer(
            train_func,
            scaling_config=scale_config,
            backend_config=CustomBackendConfig(),
        )
        trainer.fit()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
