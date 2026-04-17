import ray
import torch


@ray.remote
def hello_world():
    x = torch.ones(3)
    return f"Hello, world! torch={torch.__version__}, sum={x.sum().item()}"


def main():
    assert not torch.cuda.is_available(), "CPU image should not see a GPU"
    print(ray.get(hello_world.remote()))


if __name__ == "__main__":
    main()
