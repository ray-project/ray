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


@workflow.step
def upload_all(file_contents: List[ray.ObjectRef]) -> None:
    @workflow.step
    def upload_one(contents: str) -> None:
        upload(contents)

    children = [upload_one.step(f) for f in file_contents]

    @workflow.step
    def wait_all(*deps) -> None:
        pass

    return wait_all.step(*children)


@workflow.step
def process_all(file_contents: List[ray.ObjectRef]) -> None:
    @workflow.step
    def process_one(contents: str) -> ray.ObjectRef:
        result = process(contents)
        # Result is too large to return directly; put in the object store.
        return ray.put(result)

    children = [process_one.step(f) for f in file_contents]
    return upload_all.step(children)


@workflow.step
def download_all(urls: List[str]) -> None:
    @workflow.step
    def download_one(url: str) -> ray.ObjectRef:
        return ray.put(download(url))

    children = [download_one.step(u) for u in urls]
    return process_all.step(children)


if __name__ == "__main__":
    workflow.init()
    res = download_all.step(FILES_TO_PROCESS)
    res.run()
