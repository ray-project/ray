import argparse
import os
from collections import OrderedDict

from aws_requests_auth.boto_utils import BotoAWSRequestsAuth
import requests

parser = argparse.ArgumentParser(
    description="Helper script to upload files to S3 bucket")
parser.add_argument("--path", type=str)
parser.add_argument("--destination", type=str)
args = parser.parse_args()

assert os.path.exists(args.path)
assert args.destination in {"wheels", "containers"}
assert "BUILDKITE_JOB_ID" in os.environ
assert "BUILDKITE_COMMIT" in os.environ

is_dir = os.path.isdir(args.path)

# # Assume the caller role from the instance
# sts_client = boto3.client('sts')
# assumed_role_object = sts_client.assume_role(
#     RoleArn="arn:aws:iam::029272617770:role/presigner_caller_role",
#     RoleSessionName="ProdSessionFromBK")
# credentials = assumed_role_object["Credentials"]

# # Construct the HTTP auth to call the API gateway
# auth = AWSRequestsAuth(
#     aws_host="vop4ss7n22.execute-api.us-west-2.amazonaws.com",
#     aws_region="us-west-2",
#     aws_service="execute-api",
#     aws_access_key=credentials["AccessKeyId"],
#     aws_secret_access_key=credentials["SecretAccessKey"],
#     aws_token=credentials["SessionToken"],
# )
auth = BotoAWSRequestsAuth(
    aws_host="vop4ss7n22.execute-api.us-west-2.amazonaws.com",
    aws_region="us-west-2",
    aws_service="execute-api",
)

resp = requests.get(
    "https://vop4ss7n22.execute-api.us-west-2.amazonaws.com/endpoint/",
    auth=auth,
    params={"job_id": os.environ["BUILDKITE_JOB_ID"]})
print("Getting Presigned URL", resp.status_code)

sha = os.environ["BUILDKITE_COMMIT"]
if is_dir:
    paths = [os.path.join(args.path, f) for f in os.listdir(args.path)]
else:
    paths = [args.path]
print("Planning to upload", paths)

for path in paths:
    fn = os.path.split(path)[-1]
    if args.destination == "wheels":
        c = resp.json()["presigned_wheels"]
        of = OrderedDict(c["fields"])
        of["key"] = f"scratch/bk/{sha}/{fn}"
    elif args.destination == "containers":
        c = resp.json()["presigned_containers"]
        of = OrderedDict(c["fields"])
        of["key"] = f"{sha}/{fn}"
    else:
        raise ValueError("Unknown destination")

    of["file"] = open(path, "rb")
    r = requests.post(c["url"], files=of)
    print(f"Uploaded {path} to {of['key']}", r.status_code)
