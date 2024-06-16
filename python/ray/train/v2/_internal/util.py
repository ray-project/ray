import time


def bundle_to_remote_args(bundle: dict) -> dict:
    """Convert a bundle of resources to Ray actor/task arguments.

    >>> bundle_to_remote_args({"GPU": 1, "memory": 1, "custom": 0.1})
    {'num_cpus': 0, 'num_gpus': 1, 'memory': 1, 'resources': {'custom': 0.1}}
    """
    bundle = bundle.copy()
    args = {
        "num_cpus": bundle.pop("CPU", 0),
        "num_gpus": bundle.pop("GPU", 0),
        "memory": bundle.pop("memory", 0),
    }
    if bundle:
        args["resources"] = bundle
    return args


def time_monotonic():
    return time.monotonic()
