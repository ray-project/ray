import ray
from ray import workflow
from ray.workflow.event_listener import EventListener
from sqs_listener import SQSEventListener

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
def handle_event(*args):
    return args[0]

@workflow.step
def e1():
    return workflow.wait_for_event_revised(ExampleEventProvider, "hello")

@workflow.step
def e2():
    return workflow.wait_for_event_revised(SQSEventListener, "hello")

e3 = workflow.wait_for_event_revised(ExampleEventProvider, "hello")

@workflow.step
def w1():
    return 1

@workflow.step
def w2():
    return 2

@workflow.step
def w3():
    return 3

res = handle_event.step(e3).run()

#res = handle_event.step(workflow.wait_for_event_revised.step(ExampleEventProvider, "hello")).run()
#res = handle_event.step([e1.step(), e2.step(), w3.step()]).run()
#res = handle_event.step([w1.step(),w2.step(),w3.step()]).run()
print(res)
