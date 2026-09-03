import ray


@ray.remote
def hello_world():
    # TEMPORARY -- DO NOT MERGE. Fails the task inside the anyscale job, the way
    # a real test failure does, so that the observability agent reporter is
    # exercised on the release pipeline. Revert before taking the PR out of
    # draft.
    raise RuntimeError("Forced failure to exercise the observability agent.")
    return "Hello, world!"


def main():
    print(ray.get(hello_world.remote()))


if __name__ == "__main__":
    main()
