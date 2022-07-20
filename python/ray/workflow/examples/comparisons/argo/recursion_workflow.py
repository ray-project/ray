import ray
from ray import workflow


@ray.remote
def handle_heads() -> str:
    return "It was heads"


@ray.remote
def handle_tails() -> str:
    print("It was tails, retrying")
    return workflow.continuation(flip_coin.bind())


@ray.remote
def flip_coin() -> str:
    import random

    @ray.remote
    def decide(heads: bool) -> str:
        if heads:
            return workflow.continuation(handle_heads.bind())
        else:
            return workflow.continuation(handle_tails.bind())

    return workflow.continuation(decide.bind(random.random() > 0.5))


if __name__ == "__main__":
    print(workflow.run(flip_coin.bind()))
