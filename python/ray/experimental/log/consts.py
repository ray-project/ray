import ray.ray_constants as ray_constants

RAY_LOG_CATEGORIES = [
    "raylet",
    "gcs_server",
    "misc",
    "autoscaler",
    "runtime_env",
    "dashboard",
    "ray_client",
]

RAY_WORKER_LOG_CATEGORIES = ["worker_errors", "worker_stdout"] + sum(
    [
        [f"{lang}_driver", f"{lang}_core_worker"]
        for lang in ray_constants.LANGUAGE_WORKER_TYPES
    ],
    [],
)
