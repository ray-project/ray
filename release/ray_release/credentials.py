"""Provision fresh workload credentials for explicitly opted-in release tests."""

import base64
import json
import os
import tempfile
import uuid
from datetime import datetime, timezone
from urllib.parse import urlparse

import boto3
import requests


def prepare_release_credentials(test):
    secret_arn = test.get("cluster", {}).get("credential_config_secret")
    if not secret_arn:
        return

    api_token = os.environ.get("ANYSCALE_CLI_TOKEN")
    if not api_token:
        raise RuntimeError(
            "Anyscale CI credentials must be loaded before workload provisioning"
        )

    config = json.loads(
        boto3.client("secretsmanager", region_name="us-west-2").get_secret_value(
            SecretId=secret_arn
        )["SecretString"]
    )
    kube = config["kubernetes"]
    if urlparse(kube["server"]).scheme != "https":
        raise ValueError("The Kubernetes API must use HTTPS")
    if datetime.fromisoformat(kube["token_expires_at"].replace("Z", "+00:00")) <= (
        datetime.now(timezone.utc)
    ):
        raise RuntimeError("The release CI Kubernetes token has expired")

    assumed = boto3.client("sts", region_name="us-west-2").assume_role(
        RoleArn=config["role_arn"],
        RoleSessionName="llm-release-" + uuid.uuid4().hex[:16],
        DurationSeconds=3600,
    )["Credentials"]
    session = boto3.Session(
        aws_access_key_id=assumed["AccessKeyId"],
        aws_secret_access_key=assumed["SecretAccessKey"],
        aws_session_token=assumed["SessionToken"],
        region_name="us-west-2",
    )
    authorization = session.client("ecr").get_authorization_token(
        registryIds=["029272617770", "830883877497"]
    )["authorizationData"]
    registry_auth = {
        item["proxyEndpoint"].removeprefix("https://"): {
            "auth": item["authorizationToken"]
        }
        for item in authorization
    }
    suffix = uuid.uuid4().hex
    aws_name, image_name = "release-aws-" + suffix, "release-ecr-" + suffix
    api_name = "release-api-" + suffix
    credential_file = (
        "[release-tests]\n"
        f"aws_access_key_id = {assumed['AccessKeyId']}\n"
        f"aws_secret_access_key = {assumed['SecretAccessKey']}\n"
        f"aws_session_token = {assumed['SessionToken']}\n"
    )
    aws_config = (
        "[profile release-tests]\nregion = us-west-2\n"
        "ignore_configured_endpoint_urls = true\n"
    )
    secrets = [
        (aws_name, "Opaque", {"credentials": credential_file, "config": aws_config}),
        (
            image_name,
            "kubernetes.io/dockerconfigjson",
            {".dockerconfigjson": json.dumps({"auths": registry_auth})},
        ),
        (
            api_name,
            "Opaque",
            {"token": api_token},
        ),
    ]
    # POST creates a new object. A collision fails; we never PATCH, PUT, or DELETE.
    with tempfile.TemporaryDirectory(prefix="release-ci-ca-") as directory:
        verify = True
        if kube.get("certificate_authority_data"):
            verify = os.path.join(directory, "ca.crt")
            with open(verify, "wb") as stream:
                stream.write(base64.b64decode(kube["certificate_authority_data"]))
        for name, kind, values in secrets:
            response = requests.post(
                f"{kube['server'].rstrip('/')}/api/v1/namespaces/"
                f"{kube['namespace']}/secrets",
                headers={"Authorization": "Bearer " + kube["token"]},
                json={
                    "apiVersion": "v1",
                    "kind": "Secret",
                    "metadata": {"name": name},
                    "type": kind,
                    "stringData": values,
                },
                verify=verify,
                timeout=30,
            )
            if response.status_code != 201:
                # Do not log response bodies: they may contain Secret data.
                raise RuntimeError(
                    f"Creating release Secret {name} failed: HTTP {response.status_code}"
                )

    os.environ["RELEASE_AWS_SECRET_NAME"] = aws_name
    os.environ["RELEASE_IMAGE_PULL_SECRET_NAME"] = image_name
    os.environ["RELEASE_ANYSCALE_SECRET_NAME"] = api_name
