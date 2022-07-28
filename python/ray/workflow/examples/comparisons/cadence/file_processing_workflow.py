from typing import List

import ray
from ray import workflow

FILES_TO_PROCESS = ["file-{}".format(i) for i in range(100)]


# Mock method to download a file.
def download(url: str) -> str:
    return "contents" * 10000


# Mock method to process a file.
def process(contents: str) -> str:
    return "processed: " + contents


# Mock method to upload a file.
def upload(contents: str) -> None:
    pass


@ray.remote
def upload_all(file_contents: List[ray.ObjectRef]) -> None:
    @ray.remote
    def upload_one(contents: str) -> None:
        upload(contents)

    children = [upload_one.bind(f) for f in file_contents]

    @ray.remote
    def wait_all(*deps) -> None:
        pass

    return wait_all.bind(*children)


@ray.remote
def process_all(file_contents: List[ray.ObjectRef]) -> None:
    @ray.remote
    def process_one(contents: str) -> str:
        return process(contents)

    children = [process_one.bind(f) for f in file_contents]
    return upload_all.bind(children)


@ray.remote
def download_all(urls: List[str]) -> None:
    @ray.remote
    def download_one(url: str) -> str:
        return download(url)

    children = [download_one.bind(u) for u in urls]
    return process_all.bind(children)


if __name__ == "__main__":
    res = download_all.bind(FILES_TO_PROCESS)
    workflow.run(res)
