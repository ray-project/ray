import ray


@ray.remote
def hello_world():
    # TEMPORARY -- DO NOT MERGE. Fails the task inside the anyscale job, the way
    # a real test failure does, so that the buildkite annotation can be observed
    # on the release pipeline. Revert before merging.
    raise RuntimeError("Forced failure to exercise the buildkite annotation.")
    return "Hello, world!"


def main():
    print(ray.get(hello_world.remote()))


if __name__ == "__main__":
    main()
