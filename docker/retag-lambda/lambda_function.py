import json
import subprocess

import boto3

DOCKER_USER = None
DOCKER_PASS = None


def get_secrets():
    global DOCKER_PASS, DOCKER_USER
    secret_name = "dockerRetagLatestCredentials"
    region_name = "us-west-2"

    session = boto3.session.Session()
    client = session.client(
        service_name="secretsmanager", region_name=region_name)
    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    secret_string = get_secret_value_response["SecretString"]
    dct = json.loads(secret_string)
    DOCKER_PASS = dct["DOCKER_PASS"]
    DOCKER_USER = dct["DOCKER_USER"]


def retag(repo: str, source: str, destination: str) -> str:
    global DOCKER_PASS, DOCKER_USER
    if DOCKER_PASS is None or DOCKER_USER is None:
        get_secrets()
    assert (DOCKER_PASS is not None and
            DOCKER_USER is not None), "Docker Username or Password not set()"
    return subprocess.run(
        ["./docker-retag", f"rayproject/{repo}:{source}", destination],
        env={
            "DOCKER_USER": DOCKER_USER,
            "DOCKER_PASS": DOCKER_PASS
        })


def lambda_handler(event, context):
    source_image = event["source_tag"]
    destination_image = event["destination_tag"]
    total_results = []
    for repo in ["ray", "ray-ml", "autoscaler"]:
        results = []
        for pyversion in ["py36", "py37", "py38", "py39"]:
            source_tag = f"{source_image}-{pyversion}"
            destination_tag = f"{destination_image}-{pyversion}"
            results.append(retag(repo, source_tag, destination_tag))
            results.append(retag(repo, source_tag, destination_tag + "-cpu"))
            results.append(
                retag(repo, source_tag + "-gpu", destination_tag + "-gpu"))
        [print(i) for i in results]
        total_results.extend(results)

    # Retag images without a python version specified (defaults to py37)
    results = []
    for repo in ["ray", "ray-ml", "autoscaler", "ray-deps", "base-deps"]:
        results.append(retag(repo, source_image, destination_image))
        results.append(retag(repo, source_image, destination_image + "-cpu"))
        results.append(
            retag(repo, source_image + "-gpu", destination_image + "-gpu"))
    [print(i) for i in results]
    total_results.extend(results)

    if all(r.returncode == 0 for r in total_results):
        return {"statusCode": 200, "body": json.dumps("Retagging Complete!")}
    else:
        return {"statusCode": 500, "body": json.dumps("Retagging Broke!!")}
