import ray
import emoji


@ray.remote
def hello_world_emoji():
    return emoji.emojize(":orange_heart:")


def main():
    print(ray.get(hello_world_emoji.remote()))


if __name__ == "__main__":
    main()
