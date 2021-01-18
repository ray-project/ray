import ray
from xgboost_ray import RayParams

from _train import train_ray
from ft_small_non_elastic import FailureState, FailureInjection

if __name__ == "__main__":
    ray.init(address="auto")

    failure_state = FailureState.remote()

    ray_params = RayParams(
        elastic_training=True,
        max_failed_actors=2,
        max_actor_restarts=3,
        num_actors=4,
        cpus_per_actor=4,
        gpus_per_actor=0)

    train_ray(
        path="/data/classification.parquet",
        num_workers=4,
        num_boost_rounds=100,
        num_files=25,
        regression=False,
        use_gpu=False,
        ray_params=ray_params,
        xgboost_params=None,
        callbacks=[
            FailureInjection(state=failure_state, ranks=[3], iteration=14),
            FailureInjection(state=failure_state, ranks=[0], iteration=34),
        ])
