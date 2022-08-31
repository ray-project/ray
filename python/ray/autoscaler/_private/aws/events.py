import copy
import json
import logging
import time
from typing import Any, Dict, List

from botocore.exceptions import ClientError

from ray.autoscaler._private.aws.sns.sns_helper import SnsHelper
from ray.autoscaler._private.cli_logger import cli_logger
from ray.autoscaler._private.event_system import (
    EventCallbackHandler,
    EventPublisher,
    RayEvent,
)
from ray.autoscaler._private.updater import NodeContext

logger = logging.getLogger(__name__)


class AwsEventPublisher(EventPublisher):
    """AWS implementation of Event Publisher that allows notifications to be
    published to AWS managed services (i.e. Amazon SNS, Cloudwatch, Lambda).
    """

    def __init__(self, events_config: Dict[str, Any]):
        """Constructor for event publisher using AWS managed services.
        Args:
            events_config: A dict loaded from the autoscaler config.
        """
        super().__init__(events_config)

    def validate_config(self, events_config: Dict[str, Any]):
        notification_uri = events_config["notification_uri"]
        assert (
            notification_uri is not None
        ), "`notification_uri` is a required field in `events`"
        assert notification_uri.startswith(
            "arn:aws"
        ), f"Invalid ARN specified: {notification_uri}"

    def validate_event_base_params(self, params_config: Dict[str, Any]):
        assert params_config is not None

    def get_callback_handlers(self) -> List[EventCallbackHandler]:
        """Get callback handlers based on the provided AWS ARN.
        TODO: Add support for multiple URI?

        Returns: A list of callback handlers with their corresponding Callable
        arguments and keyword arguments
        """
        handlers = []
        if self.uri.startswith("arn:aws:sns"):
            handlers.append(
                EventCallbackHandler(
                    self._sns_callback,
                    SnsHelper(self._get_region()),
                    **self.event_base_params,
                )
            )
        elif self.uri.startswith("arn:aws:lambda"):
            handlers.append(
                EventCallbackHandler(
                    self._lambda_callback, None, **self.event_base_params
                )
            )
        elif self.uri.startswith("arn:aws:logs"):
            handlers.append(
                EventCallbackHandler(
                    self._cloudwatch_callback, None, **self.event_base_params
                )
            )
        elif self.uri.startswith("arn:aws:apigateway"):
            handlers.append(
                EventCallbackHandler(
                    self._api_gateway_callback, None, **self.event_base_params
                )
            )

        return handlers

    def _construct_sns_message(
        self, event_data: Dict[str, Any], **kwargs
    ) -> Dict[str, Any]:
        """Builds a message for Amazon SNS notifications.
        Args:
            event_data: Contains event specific data to be inserted into message
                payload.
            **kwargs: Additional parameters to insert into message payload.
        Returns: A dict representing the SNS message payload.
        """
        # create a copy of the event data to modify
        event_dict = copy.deepcopy(event_data)
        event = event_dict.pop("event_name")
        message = {
            **kwargs,
            **event_dict,
            "state": event.state,
            "stateSequence": event.value - 1,  # zero-index sequencing
            "timestamp": round(time.time() * 1000),
        }

        node_context: NodeContext = event_dict.get("node_context", {})
        if node_context:
            message["rayNodeId"] = node_context["node_id"]
            message["rayNodeType"] = (
                "HEAD" if node_context["is_head_node"] else "WORKER"
            )

        return message

    def _sns_callback(
        self, sns_client: SnsHelper, event_data: Dict[str, Any], **kwargs
    ):
        """SNS callback for sending Ray cluster event data to an SNS topic.
        Args:
            sns_client: Amazon SNS client for publishing to an SNS topic
            event_data: Ray event data which contains the event name, enum ID,
                and additional metadata (i.e. the initialization or
                setup command used during this setup step).
            **kwargs: Keyword arguments injected into the callback handler.
        """
        event: RayEvent = event_data.get("event_name")
        message = self._construct_sns_message(event_data, **kwargs)
        try:
            json_payload = json.dumps(message)
            sns_client.publish(self.uri, json_payload)
            logger.info("Published SNS event {} to {}".format(event.name, self.uri))
            logger.debug("Published SNS event payload: {}".format(json_payload))
        except ClientError as exc:
            cli_logger.abort(
                "{} Error caught when publishing {} create cluster events to SNS",
                exc.response["Error"],
                event.name,
            )

    def _lambda_callback(self):
        raise NotImplementedError("AWS Lambda callback is not implemented")

    def _cloudwatch_callback(self):
        raise NotImplementedError("AWS Cloudwatch callback is not implemented")

    def _api_gateway_callback(self):
        raise NotImplementedError("AWS API Gateway callback is not implemented")

    def _get_region(self) -> str:
        return self.uri.split(":")[3]
