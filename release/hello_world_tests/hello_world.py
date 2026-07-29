import ray


@ray.remote
def hello_world():
    return "Hello, world!"


def main():
    print(ray.get(hello_world.remote()))
    exit(1)


if __name__ == "__main__":
    main()
