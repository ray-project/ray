from ray import serve


@serve.deployment
class FooModel:
    async def __call__(self) -> None:
        return "hello"


bar = FooModel.bind()
