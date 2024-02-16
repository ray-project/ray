import requests
import json
from datetime import datetime, timezone
import subprocess
from typing import Set


def list_commit_shas():
    """
    Get list of commit SHAs on ray master branch from at least 30 days ago.
    """
    commit_shas = subprocess.check_output(
        ["git", "log", "--until='30 days ago'", "--pretty=format:%H"],
        text=True,
    )
    short_commit_shas = [commit_sha[:6] for commit_sha in commit_shas.split("\n")]
    return short_commit_shas


def get_docker_token():
    service = "registry.docker.io"
    scope = "repository:rayproject/ray:pull"
    # The URL for token authentication
    url = f"https://auth.docker.io/token?service={service}&scope={scope}"
    response = requests.get(url)
    token = response.json().get("token")
    return token


def count_docker_tags():
    """
    Count number of tags from rayproject/ray repository.
    """
    response = requests.get(
        "https://hub.docker.com/v2/namespaces/rayproject/repositories/ray/tags"
    )
    tag_count = response.json()["count"]
    return tag_count


def get_image_creation_time(repository: str, tag: str):
    """
    Get the creation time of the image from the tag image config.
    """
    res = subprocess.run(
        ["crane", "config", f"{repository}:{tag}"], capture_output=True, text=True
    )
    if res.returncode != 0 or not res.stdout:
        print("Return code: ", res.returncode)  # Replace with log
        return None
    manifest = json.loads(res.stdout)
    created = manifest["created"]
    created_time = datetime.fromisoformat(created)
    return created_time


def get_auth_token_docker_hub(username: str, password: str):
    params = {
        "username": username,
        "password": password,
    }
    headers = {
        "Content-Type": "application/json",
    }
    response = requests.post(
        "https://hub.docker.com/v2/users/login", headers=headers, params=params
    )
    token = response.json().get("token")
    return token


def delete_tags(namespace: str, repository: str, tags: list[str]):
    """
    Delete tag from Docker Hub repo.
    """
    token = get_auth_token_docker_hub("username", "password")
    headers = {
        "Authorization": f"Bearer {token}",
    }
    for tag in tags:
        print(f"Deleting {tag}")  # TODO: delete this line
        url = f"https://hub.docker.com/v2/repositories/{namespace}/{repository}/tags/{tag}"
        response = requests.delete(url, headers=headers)
        if response.status_code != 204:
            print(f"Failed to delete {tag}, status code: {response.status_code}")

def _is_release_tag(tag: str):
    """
    Check if tag is a release tag.
    """
    variables = tag.split(".")

    if len(variables) != 3 and "post1" not in tag:
        return False
    
    if not variables[0].isnumeric() or not variables[1].isnumeric():
        return False
    
    if not variables[2].isnumeric() and "rc" not in variables[2] and "-" not in variables[2]:
        return False
    
    return True

def query_release_tags(
    page_count: int, page_size: int = 100
):
    get_docker_token()
    repository = "rayproject/ray"
    with open("release_tags.txt", "w") as f:
        for page in range(page_count, 0, -1):
            print("Querying page ", page)  # Replace with log
            response = requests.get(
                "https://hub.docker.com/v2/namespaces/rayproject/repositories/ray/tags",
                params={"page": page, "page_size": 100},
            )
            # Parse tags from response
            if "errors" in response.json():
                print("Error: ", response.json()["errors"])  # Replace with log
                continue
            result = response.json()["results"]
            tag_names = [tag["name"] for tag in result]

            for tag in tag_names:
                if _is_release_tag(tag):
                    print(f"Adding {tag} to release tag list")
                    f.write(tag + "\n")

def move_tags_to_aws_ecr(tags: list[str]):
    """
    Move tags from Docker Hub to AWS ECR.
    """
    def move_tag(tag: str):
        subprocess.run(
            ["crane", "cp", f"rayproject/ray:{tag}", f"029272617770.dkr.ecr.us-west-2.amazonaws.com/rayproject/ray:{tag}"]
        )
    for tag in tags:
        move_tag(tag)

def query_tags_to_delete(
    page_count: int, commit_short_shas: Set[str], page_size: int = 100
):
    """
    Query tags to delete from rayproject/ray repository.
    """
    get_docker_token()
    repository = "rayproject/ray"
    current_time = datetime.now(timezone.utc)
    tags_to_delete = []
    for page in range(1266,1200, -1):
        print("Querying page ", page)  # Replace with log
        print("Delete count: ", len(tags_to_delete))  # Replace with log

        response = requests.get(
            "https://hub.docker.com/v2/namespaces/rayproject/repositories/ray/tags",
            params={"page": page, "page_size": 100},
        )
        if "errors" in response.json():
            continue
        result = response.json()["results"]
        tags = [tag["name"] for tag in result]
        # Check if tag is in list of commit SHAs
        commit_tags = [
            tag
            for tag in tags
            if len(tag.split("-")[0]) == 6 and tag.split("-")[0] in commit_short_shas
        ]

        for tag in commit_tags:
            created_time = get_image_creation_time(repository, tag)
            if created_time is None:
                print(f"Failed to get creation time for {tag}")  # replace with log
                continue
            time_difference = current_time - created_time
            if time_difference.days > 30:
                print(f"Deleting {tag}")  # Replace with log
                tags_to_delete.append(tag)
            else:
                return tags_to_delete
    return tags_to_delete


def main():
    page_size = 100
    # Get list of commit SHAs from at least 30 days ago
    commit_shas = list_commit_shas()
    print("Commit count: ", len(commit_shas))  # Replace with log

    docker_tag_count = count_docker_tags()
    print("Docker tag count: ", docker_tag_count)  # Replace with log

    page_count = (docker_tag_count // page_size) + 1

    #print(_is_release_tag("1.0.1.post1-gpu"))
    #query_release_tags(page_count, page_size)
    with open("release_tags_0213.txt", "r") as f:
        release_tags = f.read().split("\n")
    release_tags = set(release_tags)
    release_tags = list(release_tags)
    release_tags = sorted(release_tags)
    print("Release tag count: ", len(release_tags))
    with open("release_tags.txt", "w") as f:
        f.write("\n".join(release_tags))

    #move_tags_to_aws_ecr(important_tags)
    # important_tags = query_important_tags(page_count, page_size)
    # with open("important_tags.txt", "w") as f:
    #     f.write("\n".join(important_tags))
    # print("Page count: ", page_count)  # Replace with log
    # tags_to_delete = query_tags_to_delete(page_count, commit_shas, page_size)
    # print(len(tags_to_delete))
    # with open("tags_to_delete.txt", "w") as f:
    #     f.write("\n".join(tags_to_delete))
    # delete_tags("rayproject", "ray", tags_to_delete)


if __name__ == "__main__":
    main()
