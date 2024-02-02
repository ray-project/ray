import requests
import json
from datetime import datetime, timezone, timedelta
import re
import subprocess
from typing import Set

def list_commit_shas():
    owner = "ray-project"
    repo = "ray"
    token = "input-github-token-here"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-Github-Api_Version": "2022-11-28",
    }
    current_time = datetime.now(timezone.utc)
    time_bound = current_time - timedelta(days=30)
    time_bound = datetime.strftime(time_bound, "%Y-%m-%dT%H:%M:%SZ")
    params = {
        "sha": "master",
        "per_page": 1,
        "page": 1,
        "since": "2023-08-23T00:00:00Z",
        "until": bound
    }
    def get_commit_count():
        response = requests.get(f"https://api.github.com/repos/{owner}/{repo}/commits", headers=headers, params=params)
        pattern = r"&page=(\d+)"
        commit_count = re.findall(pattern, response.headers["Link"])[1]
        return commit_count

    commit_count = get_commit_count()
    params["per_page"] = 100
    page_count = int(commit_count) // params["per_page"] + 1
    
    commit_shas = set()
    for page in range(page_count, 0, -1):
        params["page"] = page
        response = requests.get(f"https://api.github.com/repos/{owner}/{repo}/commits", headers=headers, params=params)
        commits = response.json()
        for commit in commits:
            commit_time = commit["commit"]["author"]["date"]
            commit_time = datetime.fromisoformat(commit_time)
            time_delta = current_time - commit_time
            if time_delta.days > 30:
                commit_shas.add(commit["sha"][:6])
            else:
                return commit_shas
    return commit_shas

def get_docker_token():
    service = "registry.docker.io"
    scope = "repository:rayproject/ray:pull"
    # The URL for token authentication
    url = f"https://auth.docker.io/token?service={service}&scope={scope}"
    response = requests.get(url)
    token = response.json().get('token')
    return token

def count_docker_tags(page_size: int = 100):
    """
    Count number of tags from rayproject/ray repository.
    """
    response = requests.get('https://hub.docker.com/v2/namespaces/rayproject/repositories/ray/tags')
    tag_count = response.json()['count']
    return tag_count

def get_image_creation_time(repository: str, tag: str):
    """
    Get the creation time of the image from the tag image config.
    """
    res = subprocess.run(["crane", "config", f"{repository}:{tag}"], capture_output=True, text=True)
    if res.returncode != 0 or not res.stdout:
        return None
    manifest = json.loads(res.stdout)
    created = manifest['created']
    created_time = datetime.fromisoformat(created)

def delete_tags(namespace: str, repository: str, tags: list[str]):
    """
    Delete tag from Docker Hub repo.
    """
    for tag in tags:
        url = f"https://hub.docker.com/v2/repositories/{namespace}/{repository}/tags/{tag}"
        response = requests.delete(url)
        if response.status_code != 204:
            print(f"Failed to delete {tag}")

def query_tags_to_delete(page_size:int = 100, page_count: int, commit_short_shas: Set[str]):
    """
    Query tags to delete from rayproject/ray repository.
    """
    repository = "rayproject/ray"
    current_time = datetime.now(timezone.utc)
    tags_to_delete = []
    for page in range(page_count, 0, -1):
        response = requests.get('https://hub.docker.com/v2/namespaces/rayproject/repositories/ray/tags', params={'page': page, 'page_size': 100})
        result = response.json()["results"]
        tags = [tag["name"] for tag in result]
        commit_tags = [tag for tag in tags if len(tag.split('-')[0]) == 6 and tag.split('-')[0] in far_commits]
        
        for tag in commit_tags:
            created_time = get_image_creation_time(repository, tag)
            if created_time is None:
                print(f"Failed to get creation time for {tag}") # replace with log
                continue
            time_difference = current_time - created_time
            if time_difference.days > 30:
                tags_to_delete.append(tag)
            else:
                return tags_to_delete
    return tags_to_delete


def main():
    commit_shas = list_commit_shas()
    token = get_docker_token()
    docker_tag_count = count_docker_tags(100)
    page_count = tag_count // page_size + 1
    tags_to_delete = query_tags_to_delete(page_size, page_count, commit_shas)
    # delete_tags("rayproject", "ray", tags_to_delete)
if __name__ == '__main__':
    main()
