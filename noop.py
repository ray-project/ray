from ray import serve

from starlette.responses import StreamingResponse

@serve.deployment
class A:
    def __init__(self):
        import logging
        logger = logging.getLogger("ray.serve")
        logger.setLevel(logging.ERROR)

    def hi(self):
        for i in range(5):
            yield str(i)

    def __call__(self, *args) -> StreamingResponse:
        return StreamingResponse(self.hi())

app = A.bind()
