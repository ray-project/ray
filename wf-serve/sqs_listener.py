import aioboto3
from ray import workflow


class SQSEventListener(workflow.EventListener):
    def __init__(self):
        self.session = aioboto3.Session(region_name='us-west-2')
        self.url = "https://sqs.us-west-2.amazonaws.com/959243851260/job-queue"

    async def poll_for_event(self):
        async with self.session.client('sqs') as sqs:
            while True:
                print("!!!")
                ret = await sqs.receive_message(
                    QueueUrl=self.url, WaitTimeSeconds=20)
                if ret.get("Messages") is not None:
                    return ret

    async def event_checkpointed(self, event) -> None:
        try:
            entries = [{
                'Id': e['MessageId'],
                'ReceiptHandle': e['ReceiptHandle']
            } for e in event['Messages']]
        except Exception as e:
            print("Error", e)
            return
        async with self.session.client('sqs') as sqs:
            await sqs.delete_message_batch(QueueUrl=self.url, Entries=entries)
