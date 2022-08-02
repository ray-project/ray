import asyncio
from typing import Dict
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

import ray
from ray import serve
from ray.workflow import common, workflow_context, workflow_access
from ray.workflow.event_listener import EventListener
from ray.workflow.common import Event


import logging

logger = logging.getLogger(__name__)


class WorkflowEventHandleError(Exception):
    """Raise when event processing failed"""

    def __init__(self, workflow_id: str, what_happened: str):
        self.message = (
            f"Workflow[id={workflow_id}] HTTP event handle failed: {what_happened}"
        )
        super().__init__(self.message)


app = FastAPI()


@serve.deployment(
    name=common.HTTP_EVENT_PROVIDER_NAME, route_prefix="/event", num_replicas=1
)
@serve.ingress(app)
class HTTPEventProvider:
    """HTTPEventProvider is defined to be a Ray Serve deployment with route_prefix='/event',
    which will receive external events via an HTTP endpoint. It supports FastAPI,
    e.g. post. It responds to both poll_for_event() and event_checkpointed() from
    an HTTPListener instance.

    HTTPListener class is designed to work with the current workflow.wait_for_event()
    implementation, where an HTTPListener instance will be initiated by the
    get_message() and message_committed() of the workflow.wait_for_event().

    HTTPEventProvider requires an event to arrive after HTTPListner registers
    its event_key. If an event arrived before the registration, it returns HTTP
    error code 404 with the error "workflow_id and event_key need to be registered
    to receive event. Please make sure they are registered before resending."

    Example definition
    ==================

    ```
    class HTTPEventProvider:

        def __init__(self):

        @app.post("/send_event/{workflow_id}")
        async def send_event(self, workflow_id: str, req: Request):
            Receive an external event message and acknowledge if it was processed
            by the workflow
        async def get_event_payload(self, workflow_id, event_key):
            Internal method used by HTTPListner to subscribe to an event matched by
            workflow_id and event_key
        async def report_checkpointed(self, workflow_id, event, confirmation):
            Internal method used by HTTPListner to confirm the received event has been
            checkpointed by workflow
    ```

    Example Usage
    =============
    >>> from ray.workflow.http_event_provider import HTTPEventProvider, HTTPListener
    >>> ray.init(address='auto', namespace='serve')
    >>> serve.start(detached=True)
    >>> event_node = workflow.wait_for_event( # doctest: +SKIP
    ...     HTTPListener, event_key='')
    >>> handle_event = ... # doctest: +SKIP
    >>> workflow.run_aync(handle_event.bind(event_node)) # doctest: +SKIP
    >>>
    >>> On a separate python process, it sends an event to the HTTPEventProvider.
    >>> import requests
    >>> resp = requests.post('http://127.0.0.1:8000/event/send_event/{workflow_id}',
    ...     json={'event_key':'my_key','event_payload':'testMessage'})
    >>>

    """

    def __init__(self):
        """Maintain two data structures to track pending events and confirmations
        event_key_payload: for each registered workflow_id and event_key,
            keep the Future to be set after an event is received.
        event_checkpoint_pending: for each received event_key, keep its Future
            after checkpointing is confirmed so HTTP 200 can be returned.
        """
        self.event_key_payload: Dict[str, Dict[str, asyncio.Future]] = {}
        self.event_checkpoint_pending: Dict[str, asyncio.Future] = {}

    @app.post("/send_event/{workflow_id}")
    async def send_event(self, workflow_id: str, req: Request) -> JSONResponse:
        """Receive an external event message and acknowledge if it was processed
        by the workflow
        Args:
            workflow_id: the workflow that this event is submitted for
            req: the JSON formatted request that contains two string fields: '
            event_key' and 'event_payload'
                'event_key' uniquely identifies a node in the receiving workflow;
                'event_payload' refers to the event's content
        Example:
            JSON formatted request {"event_key":"node_event","event_payload":"approved"}
        Returns:
            if the event was received and processed, HTTP response status 200
            if the event was not expected or the workflow_id did not exist, HTTP
                response status 404
            if the event was received but failed at checkpointing, HTTP response 500

        """
        req_json = await req.json()
        try:
            event_key = req_json["event_key"]
            event_payload = req_json["event_payload"]
        except KeyError as e:
            return JSONResponse(
                status_code=404,
                content={
                    "error": {
                        "code": 404,
                        "message": f"{e} field is not found in the request JSON",
                    }
                },
            )
        try:
            self.event_key_payload[workflow_id][event_key].set_result(
                (event_key, event_payload)
            )
        except KeyError:
            return JSONResponse(
                status_code=404,
                content={
                    "error": {
                        "code": 404,
                        "message": "workflow_id and event_key need to be registered "
                        "to receive event. Please make sure they are "
                        "registered before resending.",
                    }
                },
            )

        self.event_checkpoint_pending[event_key] = asyncio.Future()
        confirmed = await self.event_checkpoint_pending[event_key]
        self.event_checkpoint_pending.pop(event_key)
        if confirmed:
            return JSONResponse(status_code=200, content={})
        return JSONResponse(
            status_code=500,
            content={"error": {"code": 500, "message": "event processing failed"}},
        )

    async def get_event_payload(self, workflow_id: str, event_key: str) -> Event:
        """Internal method used by HTTPListener to subscribe to an event matched
        by workflow_id and event_key"""
        if workflow_id not in self.event_key_payload:
            self.event_key_payload[workflow_id] = {}

        if event_key in self.event_key_payload[workflow_id]:
            raise WorkflowEventHandleError(
                workflow_id, f"The same {event_key} is used to get payload again."
            )

        self.event_key_payload[workflow_id][event_key] = asyncio.Future()
        return await self.event_key_payload[workflow_id][event_key]

    async def report_checkpointed(
        self, workflow_id: str, event_key: str, confirmation: bool
    ) -> str:
        """Internal method used by HTTPListner to confirm the received event has
        been checkpointed by workflow"""
        try:
            self.event_checkpoint_pending[event_key].set_result(confirmation)
        except KeyError:
            logger.error(
                f"{event_key} cannot be found to acknowledge request. "
                f"The event provider may have been restarted."
            )
            raise WorkflowEventHandleError(
                workflow_id, f"{event_key} cannot be found to acknowledge request."
            )
        return "OK"


class HTTPListener(EventListener):
    """HTTPLister is defined to work with the HTTPEventProvider. It implements two
    APIs, poll_for_event() and event_checkpointed(). An instance of HTTPListener will
    be started by the get_message() of the workflow.wait_for_event() to listen for
    an event from the HTTPEventProvider instance (a Ray Serve deployment). Another
    instance of HTTPListener will be started by the message_committed() of the
    workflow.wait_for_event() to confirm that the event has been checkpointed.


    Example definition
    ==================

    ```
    class HTTPListener:

        def __init__(self):

        async def poll_for_event(self, event_key) -> Event:

        async def event_checkpointed(self, event) -> None:

    ```

    Example Usage
    =============
    >>> from ray.workflow.http_event_provider import HTTPEventProvider, HTTPListener
    >>> ray.init(address='auto', namespace='serve')
    >>> serve.start(detached=True)
    >>> event_node = workflow.wait_for_event( # doctest: +SKIP
    ...     HTTPListener, event_key='')
    >>> handle_event = ... # doctest: +SKIP
    >>> workflow.run(handle_event.bind(event_node)) # doctest: +SKIP
    >>>

    """

    def __init__(self):
        super().__init__()
        try:
            self.handle = ray.serve.get_deployment(
                common.HTTP_EVENT_PROVIDER_NAME
            ).get_handle(sync=True)
        except KeyError:
            mgr = workflow_access.get_management_actor()
            ray.get(mgr.create_http_event_provider.remote())
            self.handle = ray.serve.get_deployment(
                common.HTTP_EVENT_PROVIDER_NAME
            ).get_handle(sync=True)

    async def poll_for_event(self, event_key: str = None) -> Event:
        """workflow.wait_for_event calls this method to subscribe to the
        HTTPEventProvider and return the received external event
        Args:
            event_key: a unique identifier to the receiving node in a workflow;
            if missing, default to current workflow task id
        Returns:
            tuple(event_key, event_payload)
        """
        workflow_id = workflow_context.get_current_workflow_id()
        if event_key is None:
            event_key = workflow_context.get_current_task_id()

        event_key_payload = await self.handle.get_event_payload.remote(
            workflow_id, event_key
        )
        return event_key_payload

    async def event_checkpointed(self, event: Event) -> None:
        """workflow.wait_for_event calls this method after the event has
        been checkpointed and a transaction can be safely committed."""
        (event_key, _) = event
        await self.handle.report_checkpointed.remote(
            workflow_context.get_current_workflow_id(), event_key, True
        )
