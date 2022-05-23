import os
import ray
import sys

RAY_VERSION = "RAY_VERSION"
RAY_COMMIT = "RAY_HASH"

ray_version = os.getenv(RAY_VERSION)
ray_commit = os.getenv(RAY_COMMIT)

if __name__ == "__main__":
    print("Sanity check python version: {}".format(sys.version))
    assert (
        ray_version == ray.__version__
    ), "Given Ray version {} is not matching with downloaded " "version {}".format(
        ray_version, ray.__version__
    )
    assert (
        ray_commit == ray.__commit__
    ), "Given Ray commit {} is not matching with downloaded " "version {}".format(
        ray_commit, ray.__commit__
    )
    assert ray.__file__ is not None

    ray.init()
    assert ray.is_initialized()

    @ray.remote
    def return_arg(arg):
        return arg

    val = 3
    print("Running basic sanity check.")
    assert ray.get(return_arg.remote(val)) == val
    ray.shutdown()
