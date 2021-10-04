import json
import subprocess


def retag(repo: str, source: str, destination: str) -> str:
    return subprocess.run(
        ["./docker-retag", f"rayproject/{repo}:{source}", destination])


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
