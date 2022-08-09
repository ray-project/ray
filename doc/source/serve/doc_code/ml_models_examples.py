from ray import serve

# __batch_example_start__
@serve.deployment(route_prefix="/increment")
class BatchingExample:
    def __init__(self):
        self.count = 0

    @serve.batch
    async def handle_batch(self, requests):
        responses = []
        for request in requests:
            responses.append(request.json())

        return responses

    async def __call__(self, request):
        return await self.handle_batch(request)


serve.run(BatchingExample.bind())
# __batch_example_end__
