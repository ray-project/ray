#!/usr/bin/env python3

import json
import sys


def print_aws_envs():
    input = sys.stdin.read()
    creds_json = json.loads(input)
    creds = creds_json["Credentials"]
    print("export AWS_ACCESS_KEY_ID={}".format(creds["AccessKeyId"]))
    print("export AWS_SECRET_ACCESS_KEY={}".format(creds["SecretAccessKey"]))
    print("export AWS_SESSION_TOKEN={}".format(creds["SessionToken"]))


if __name__ == "__main__":
    print_aws_envs()
