import logging
from typing import Any, Dict

from ray.autoscaler._private.aws.utils import client_cache

logger = logging.getLogger(__name__)


class SnsHelper:
    def __init__(self, region: str, **kwargs):
        """Constructor for SNS helper class. Re-uses the SNS client from
        the resource cache, if present.

        Args:
            region: The AWS Region used in instantiating the client.
            **kwargs: Additional keyword arguments specified when loading the
                SNS client from the resource cache.
        """
        self.sns = client_cache("sns", region, **kwargs)

    def subscribe(self, topic_arn: str) -> str:
        """Subscribe to an SNS topic.

        Args:
            topic_arn: SNS topic ARN.
        Returns: A string representing the subscription ARN.
        """
        response = self.sns.subscribe(TopicArn=topic_arn, Protocol="https")
        return response["SubscriptionArn"]

    def publish(self, topic_arn: str, message: str) -> Dict[str, Any]:
        """Publish a message to an SNS topic.

        Args:
            topic_arn: SNS topic ARN.
            message: SNS message payload.
        Returns: A deserialized JSON response object from SNS.
        """
        response = self.sns.publish(TopicArn=topic_arn, Message=message)
        return response
