import json
import os
import subprocess

import boto3

DOCKER_USER = None
DOCKER_PASS = None


def _get_curr_dir():
    return os.path.dirname(os.path.realpath(__file__))


def get_secrets():
    global DOCKER_PASS, DOCKER_USER
    secret_name = "dockerRetagLatestCredentials"
    region_name = "us-west-2"

    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)
    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    secret_string = get_secret_value_response["SecretString"]
    dct = json.loads(secret_string)
    DOCKER_PASS = dct["DOCKER_PASS"]
    DOCKER_USER = dct["DOCKER_USER"]


def retag(repo: str, source: str, destination: str) -> str:
    global DOCKER_PASS, DOCKER_USER
    if DOCKER_PASS is None or DOCKER_USER is None:
        get_secrets()
    assert (
        DOCKER_PASS is not None and DOCKER_USER is not None
    ), "Docker Username or Password not set()"
    return subprocess.run(
        ["./docker-retag", f"rayproject/{repo}:{source}", destination],
        env={"DOCKER_USER": DOCKER_USER, "DOCKER_PASS": DOCKER_PASS},
    )


def parse_versions(version_file):
    with open(version_file) as f:
        file_versions = f.read().splitlines()
    return file_versions


def lambda_handler(event, context):
    source_image = event["source_tag"]
    destination_image = event["destination_tag"]
    total_results = []
    python_versions = parse_versions(
        os.path.join(_get_curr_dir(), "python_versions.txt")
    )
    cuda_versions = parse_versions(os.path.join(_get_curr_dir(), "cuda_versions.txt"))
    for repo in ["ray", "ray-ml"]:
        results = []
        # For example tag ray:1.X-py3.7-cu112 to ray:latest-py3.7-cu112
        for pyversion in python_versions:
            source_tag = f"{source_image}-{pyversion}"
            destination_tag = f"{destination_image}-{pyversion}"
            for cudaversion in cuda_versions:
                cuda_source_tag = source_tag + f"-{cudaversion}"
                cuda_destination_tag = destination_tag + f"-{cudaversion}"
                results.append(retag(repo, cuda_source_tag, cuda_destination_tag))
            # Tag ray:1.X-py3.7 to ray:latest-py3.7
            results.append(retag(repo, source_tag, destination_tag))
            # Tag ray:1.X-py3.7-cpu to ray:latest-py3.7-cpu
            results.append(retag(repo, source_tag + "-cpu", destination_tag + "-cpu"))
            # Tag ray:1.X-py3.7-gpu to ray:latest-py3.7-gpu
            results.append(retag(repo, source_tag + "-gpu", destination_tag + "-gpu"))
        [print(i) for i in results]
        total_results.extend(results)

    # Retag images without a python version specified (defaults to py37)
    results = []
    for repo in ["ray", "ray-ml", "ray-deps", "base-deps"]:
        # For example tag ray:1.X-cu112 to ray:latest-cu112
        for cudaversion in cuda_versions:
            source_tag = f"{source_image}-{cudaversion}"
            destination_tag = f"{destination_image}-{cudaversion}"
            results.append(retag(repo, source_tag, destination_tag))

        # Tag ray:1.X to ray:latest
        results.append(retag(repo, source_image, destination_image))
        # Tag ray:1.X-cpu to ray:latest-cpu
        results.append(retag(repo, source_image + "-cpu", destination_image + "-cpu"))
        # Tag ray:1.X-gpu to ray:latest-gpu
        results.append(retag(repo, source_image + "-gpu", destination_image + "-gpu"))
    [print(i) for i in results]
    total_results.extend(results)

    if all(r.returncode == 0 for r in total_results):
        return {"statusCode": 200, "body": json.dumps("Retagging Complete!")}
    else:
        return {"statusCode": 500, "body": json.dumps("Retagging Broke!!")}
