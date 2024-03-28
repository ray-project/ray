from ray import serve


@serve.deployment
class Noop:
    async def __call__(self):
        return 0


app = Noop.bind()
