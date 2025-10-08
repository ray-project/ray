import ray


@ray.remote
def hello_world():
    return "Hello, world!"


def main():
    print(ray.get(hello_world.remote()))
    assert 1 == 2


if __name__ == "__main__":
    main()
