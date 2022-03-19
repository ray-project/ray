import ray
from ray import workflow
from ray.workflow.event_listener import EventListener

ray.init(address='auto')
workflow.init()

class ExampleEventProvider(EventListener):
    def __init__(self):
        pass 

    async def poll_for_event(self, *args):
        pass

    async def event_checkpointed(self, *args):
        pass


@workflow.step
def handle_event(msg):
    print(msg)

event_func = workflow.wait_for_event_revised(ExampleEventProvider, "hello")
handle_event.step(event_func).run()


