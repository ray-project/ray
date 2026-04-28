import ray
import torch


@ray.remote
def hello_world():
    if torch.cuda.is_available():
        x = torch.ones(3, device="cuda")
        return (
            f"Hello, world! torch={torch.__version__}, "
            f"cuda={torch.version.cuda}, device={torch.cuda.get_device_name(0)}, "
            f"sum={x.sum().item()}"
        )
    x = torch.ones(3)
    return f"Hello, world! torch={torch.__version__}, sum={x.sum().item()}"


def main():
    print(ray.get(hello_world.remote()))


if __name__ == "__main__":
    main()
