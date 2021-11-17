import boto3
sqs = boto3.client('sqs', region_name='us-west-2')
import uuid
queue_url = 'https://sqs.us-west-2.amazonaws.com/959243851260/job-queue'

# Send message to SQS queue
response = sqs.send_message(QueueUrl=queue_url, MessageBody=str(uuid.uuid4()))

print(response['MessageId'])
