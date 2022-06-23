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


# See test_client::test_wrapped_actor_creation for details on usage of
# run_wrapped_actor_creation and SomeClass.
def run_wrapped_actor_creation():
    import ray

    RemoteClass = ray.remote(SomeClass)
    handle = RemoteClass.remote()
    return ray.get(handle.ready.remote())


class SomeClass:
    def __init__(self):
        pass

    def ready(self):
        return 1
