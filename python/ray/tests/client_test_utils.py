import asyncio


def create_remote_signal_actor(ray):
    # TODO(barakmich): num_cpus=0
    @ray.remote
    class SignalActor:
        def __init__(self):
            self.ready_event = asyncio.Event()

        def send(self, clear=False):
            self.ready_event.set()
            if clear:
                self.ready_event.clear()

        async def wait(self, should_wait=True):
            if should_wait:
                await self.ready_event.wait()

    return SignalActor
