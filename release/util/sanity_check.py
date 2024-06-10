import click
import ray
import sys


@click.command()
@click.option("--ray_version", required=True, type=str)
@click.option("--ray_commit", required=True, type=str)
def main(ray_version, ray_commit):
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
    print("Sanity check succeeded on Python {}".format(sys.version))


if __name__ == "__main__":
    main()
