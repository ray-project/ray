import argparse
import asyncio
import logging

import requests
from fastapi import FastAPI
from starlette.responses import StreamingResponse

import ray
from ray import serve

logger = logging.getLogger("ray.serve")

fastapi_app = FastAPI()


# Input: a prompt of words
# Output: each word reversed and produced N times.
@serve.deployment(
    num_replicas=6, ray_actor_options={"num_cpus": 0.01, "memory": 10 * 1024 * 1024}
)
class ReverseAndDupEachWord:
    def __init__(self, dup_times: int):
        self.dup_times = dup_times

    async def __call__(self, prompt: str):
        for word in prompt.split():
            rev = word[::-1]
            for _ in range(self.dup_times):
                await asyncio.sleep(0.001)
                # Ideally we want to do " ".join(words), but for the sake of
                # simplicity we also have an extra trailing space.
                yield rev + " "


@serve.deployment(
    num_replicas=6, ray_actor_options={"num_cpus": 0.01, "memory": 10 * 1024 * 1024}
)
@serve.ingress(fastapi_app)
class Textbot:
    def __init__(self, llm):
        self.llm = llm.options(stream=True)

    @fastapi_app.post("/")
    async def handle_request(self, prompt: str) -> StreamingResponse:
        logger.info(f'Got prompt with size "{len(prompt)}"')
        return StreamingResponse(self.llm.remote(prompt), media_type="text/plain")


@ray.remote(num_cpus=0.1, memory=10 * 1024 * 1024)
def make_http_query(num_words, num_queries):
    for _ in range(num_queries):
        words = "Lorem ipsum dolor sit amet".split()
        prompt_words = [words[i % len(words)] for i in range(num_words)]
        prompt = " ".join(prompt_words)
        expected_words = [word[::-1] for word in prompt_words for _ in range(2)]

        response = requests.post(f"http://localhost:8000/?prompt={prompt}", stream=True)
        response.raise_for_status()
        content = response.content.decode()
        assert content == " ".join(expected_words) + " ", content


def main():
    parser = argparse.ArgumentParser(description="Generates HTTP workloads with Ray.")

    parser.add_argument("--num_tasks", type=int, required=True, help="Number of tasks.")
    parser.add_argument(
        "--num_queries_per_task",
        type=int,
        required=True,
        help="Number of queries per task.",
    )
    parser.add_argument(
        "--num_words_per_query",
        type=int,
        required=True,
        help="Number of words per query",
    )

    args = parser.parse_args()

    # Run the serve, run the client, then showdown serve.
    llm = ReverseAndDupEachWord.bind(2)
    app = Textbot.bind(llm)

    serve.run(app)

    objs = [
        make_http_query.remote(args.num_words_per_query, args.num_queries_per_task)
        for _ in range(args.num_tasks)
    ]
    ray.get(objs)

    serve.shutdown()


main()
