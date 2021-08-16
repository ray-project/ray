from typing import List

import ray
from ray import workflow


@workflow.step
def upload_all(file_contents: List[ray.ObjectRef]) -> None:
    @workflow.step
    def upload_one(ref: ray.ObjectRef) -> None:
        import custom_processing
        custom_processing.upload(ray.get(ref))

    children = [upload_one.step(f) for f in file_contents]

    @workflow.step
    def wait_all(*deps) -> None:
        pass

    return wait_all.step(*children)


@workflow.step
def process_all(file_contents: List[ray.ObjectRef]) -> None:
    @workflow.step
    def process_one(ref: ray.ObjectRef) -> ray.ObjectRef:
        import custom_processing
        result = custom_processing.process(ray.get(ref))
        # Result is too large to return directly; put in the object store.
        return ray.put(result)

    children = [process_one.step(f) for f in file_contents]
    return upload_all.step(children)


@workflow.step
def download_all(urls: List[str]) -> None:
    @workflow.step
    def download_one(url: str) -> ray.ObjectRef:
        import requests
        # Result is too large to return directly; put in the object store.
        return ray.put(requests.get(url).text)

    children = [download_one.step(u) for u in urls]
    return process_all.step(children)


if __name__ == "__main__":
    workflow.init()

    import custom_processing

    res = download_all.step(custom_processing.files_to_process())
    res.run()
