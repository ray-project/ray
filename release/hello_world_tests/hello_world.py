import ray


@ray.remote
def hello_world():
    # TEMPORARY -- DO NOT MERGE. Fails the task inside the anyscale job, the way
    # a real test failure does, so that the retry budget of a test configured
    # with num_retries can be exercised on the release pipeline. Revert before
    # merging.
    raise RuntimeError("Forced failure to exercise the retry budget.")
    return "Hello, world!"


def main():
    print(ray.get(hello_world.remote()))


if __name__ == "__main__":
    main()
