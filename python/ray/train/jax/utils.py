from ray.air.config import ScalingConfig

def ensure_tpu_resources_capitalized(scaling_config: ScalingConfig) -> ScalingConfig:
    """Ensure that the resources_per_worker has capitalized keys."""
    # case-insensitivize
    # since `tpu` is not the standard resources in ray currently
    # add these lines to prevent the cases where the users
    # give the lower-case `tpu` as the resources
    # and change the key to upper case!
    resources_per_worker = scaling_config.resources_per_worker
    if resources_per_worker:
        resources_per_worker_upper = {}
        for k, v in resources_per_worker.items():
            if k.upper() == "TPU": 
                resources_per_worker_upper[k.upper()] = v
            else:                     
                resources_per_worker_upper[k] = v
        scaling_config.resources_per_worker = resources_per_worker_upper
    return scaling_config