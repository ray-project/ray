import ray


@ray.remote
def hello_world():
    raise RuntimeError("Hello, world!")


def main():
    print(ray.get(hello_world.remote()))


if __name__ == "__main__":
    main()
