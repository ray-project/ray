from ray import workflow


@workflow.step
def handle_heads() -> str:
    return "It was heads"


@workflow.step
def handle_tails() -> str:
    print("It was tails, retrying")
    return flip_coin.step()


@workflow.step
def flip_coin() -> str:
    import random

    @workflow.step
    def decide(heads: bool) -> str:
        if heads:
            return handle_heads.step()
        else:
            return handle_tails.step()

    return decide.step(random.random() > 0.5)


if __name__ == "__main__":
    workflow.init()
    print(flip_coin.step().run())
