import ray
import torch


@ray.remote(num_gpus=1)
def hello_world():
    device = torch.device("cuda")
    x = torch.ones(3, device=device)
    return (
        f"Hello, world! torch={torch.__version__}, "
        f"cuda={torch.version.cuda}, device={torch.cuda.get_device_name(0)}, "
        f"sum={x.sum().item()}"
    )


def main():
    assert torch.cuda.is_available(), "GPU image must see a CUDA device"
    print(ray.get(hello_world.remote()))


if __name__ == "__main__":
    main()
