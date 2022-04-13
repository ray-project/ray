from sklearn.base import BaseEstimator

# thread_count is a catboost parameter
SKLEARN_CPU_PARAM_NAMES = ["n_jobs", "thread_count"]


def has_cpu_params(estimator: BaseEstimator) -> bool:
    """Returns True if estimator has any CPU-related params."""
    return any(
        any(
            param.endswith(cpu_param_name) for cpu_param_name in SKLEARN_CPU_PARAM_NAMES
        )
        for param in estimator.get_params(deep=True)
    )


def set_cpu_params(estimator: BaseEstimator, num_cpus: int) -> None:
    """Sets all CPU-related params to num_cpus (incl. nested)."""
    cpu_params = {
        param: num_cpus
        for param in estimator.get_params(deep=True)
        if any(
            param.endswith(cpu_param_name) for cpu_param_name in SKLEARN_CPU_PARAM_NAMES
        )
    }
    estimator.set_params(**cpu_params)
