import ray
import asyncio
import requests
from requests import Request
from ray import serve
from typing import Dict, List, Optional, Any, Callable, Tuple, Union, Set
from fastapi import FastAPI, Request



class WorkflowEventHandleError(Exception):
    def __init__(self, workflow_id: str, what_happen: str):
        self.message = f"Workflow[id={workflow_id}] HTTP event handle failed: {what_happen}"
        super().__init__(self.message)

app = FastAPI()
ray.init(address='auto', namespace='serve')
serve.start(detached=True)

@serve.deployment(name="myEventProvider", route_prefix="/event")
@serve.ingress(app)
class CustomHTTPEventProvider():
    def __init__(self):
        import nest_asyncio
        nest_asyncio.apply()
        self.event_key_payload: Dict[str, Dict[Any, Any]] = {}

    # An alternative is to use
    # @app.get("/arrivals")
    # async def create_event_payload(self, workflow_id: str, event_key: Any, event_payload: Any):
    @app.post("/arrivals")
    async def create_event_payload(self, req: Request):
        req_json = await req.json()
        workflow_id = req_json['workflow_id']
        event_key = req_json['event_key']
        event_payload = req_json['event_payload']
        if workflow_id not in self.event_key_payload.keys():
            return f"Event Dropped: It is not registered. Please resend after it is registered."
        if event_key not in self.event_key_payload[workflow_id].keys():
            return f"Event Dropped: It is not registered. Please resend after it is registered."
        self.event_key_payload[workflow_id][event_key].set_result(event_payload)
        return f"ACK_OK"

    async def get_event_payload(self, workflow_id, event_key):
        if workflow_id not in self.event_key_payload.keys():
            self.event_key_payload[workflow_id] = {}
        if event_key in self.event_key_payload[workflow_id]:
            raise WorkflowEventHandleError(workflow_id, f"The same {event_key} is used to get payload again.")
        self.event_key_payload[workflow_id][event_key] = asyncio.Future()
        return await self.event_key_payload[workflow_id][event_key]


class HTTPListener():
    def __init__(self):
        from ray import serve
        self.handle = ray.serve.get_deployment('myEventProvider').get_handle(sync=True)

    async def poll_for_event(self, workflow_id, event_key):
        payload = await self.handle.get_event_payload.remote(workflow_id, event_key)
        return payload

    async def event_checkpointed(self, workflow_id: str, event_key: Any) -> None:
        """Optional. Called after an event has been checkpointed and a transaction can
        be safely committed."""
        # perform event maintenance
        pass

CustomHTTPEventProvider.deploy()

async def __main__(*args, **kwargs):
    import nest_asyncio
    nest_asyncio.apply()

    httplistener = HTTPListener()

    from datetime import datetime

    print(f"start poll_for_event() at {datetime.now().strftime('%H:%M:%S')}")
    payload = await httplistener.poll_for_event("wf", "my_key")
    print(f"payload: {payload} returned at {datetime.now().strftime('%H:%M:%S')}")

asyncio.run(__main__())
