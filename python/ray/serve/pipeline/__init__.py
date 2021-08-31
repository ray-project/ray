import ray

class Sequential:
    def __init__(self, *stages):
        for stage in stages:
            assert isinstance(stage, ray.serve.api.Deployment)
        self._stages = [stage.get_handle(sync=False) for stage in stages]

    async def remote(self, *args, **kwargs):
        prev_ref = await self._stages[0].remote()
        for stage in self._stages[1:]:
            prev_ref = await stage.remote(prev_ref)

        return await prev_ref
