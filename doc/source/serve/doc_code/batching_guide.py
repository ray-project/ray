# flake8: noqa
# __single_sample_begin__
from ray import serve
from ray.serve.handle import DeploymentHandle


@serve.deployment
class Model:
    def __call__(self, single_sample: int) -> int:
        return single_sample * 2


handle: DeploymentHandle = serve.run(Model.bind())
assert handle.remote(1).result() == 2
# __single_sample_end__


# __batch_begin__
from typing import List

import numpy as np

from ray import serve
from ray.serve.handle import DeploymentHandle


@serve.deployment
class Model:
    @serve.batch(max_batch_size=8, batch_wait_timeout_s=0.1)
    async def __call__(self, multiple_samples: List[int]) -> List[int]:
        # Use numpy's vectorized computation to efficiently process a batch.
        return np.array(multiple_samples) * 2


handle: DeploymentHandle = serve.run(Model.bind())
responses = [handle.remote(i) for i in range(8)]
assert list(r.result() for r in responses) == [i * 2 for i in range(8)]
# __batch_end__


# __batch_params_update_begin__
from typing import Dict


@serve.deployment(
    # These values can be overridden in the Serve config.
    user_config={
        "max_batch_size": 10,
        "batch_wait_timeout_s": 0.5,
    }
)
class Model:
    @serve.batch(max_batch_size=8, batch_wait_timeout_s=0.1)
    async def __call__(self, multiple_samples: List[int]) -> List[int]:
        # Use numpy's vectorized computation to efficiently process a batch.
        return np.array(multiple_samples) * 2

    def reconfigure(self, user_config: Dict):
        self.__call__.set_max_batch_size(user_config["max_batch_size"])
        self.__call__.set_batch_wait_timeout_s(user_config["batch_wait_timeout_s"])


# __batch_params_update_end__


# __single_stream_begin__
import asyncio
from typing import AsyncGenerator
from starlette.requests import Request
from starlette.responses import StreamingResponse

from ray import serve


@serve.deployment
class StreamingResponder:
    async def generate_numbers(self, max: str) -> AsyncGenerator[str, None]:
        for i in range(max):
            yield str(i)
            await asyncio.sleep(0.1)

    def __call__(self, request: Request) -> StreamingResponse:
        max = int(request.query_params.get("max", "25"))
        gen = self.generate_numbers(max)
        return StreamingResponse(gen, status_code=200, media_type="text/plain")


# __single_stream_end__

import requests

serve.run(StreamingResponder.bind())
r = requests.get("http://localhost:8000/", stream=True)
chunks = []
for chunk in r.iter_content(chunk_size=None, decode_unicode=True):
    chunks.append(chunk)

assert ",".join(list(map(str, range(25)))) == ",".join(chunks)

# __batch_stream_begin__
import asyncio
from typing import List, AsyncGenerator, Union
from starlette.requests import Request
from starlette.responses import StreamingResponse

from ray import serve


@serve.deployment
class StreamingResponder:
    @serve.batch(max_batch_size=5, batch_wait_timeout_s=0.1)
    async def generate_numbers(
        self, max_list: List[str]
    ) -> AsyncGenerator[List[Union[int, StopIteration]], None]:
        for i in range(max(max_list)):
            next_numbers = []
            for requested_max in max_list:
                if requested_max > i:
                    next_numbers.append(str(i))
                else:
                    next_numbers.append(StopIteration)
            yield next_numbers
            await asyncio.sleep(0.1)

    async def __call__(self, request: Request) -> StreamingResponse:
        max = int(request.query_params.get("max", "25"))
        gen = self.generate_numbers(max)
        return StreamingResponse(gen, status_code=200, media_type="text/plain")


# __batch_stream_end__

import requests
from functools import partial
from concurrent.futures.thread import ThreadPoolExecutor

serve.run(StreamingResponder.bind())


def issue_request(max) -> List[str]:
    url = "http://localhost:8000/?max="
    response = requests.get(url + str(max), stream=True)
    chunks = []
    for chunk in response.iter_content(chunk_size=None, decode_unicode=True):
        chunks.append(chunk)
    return chunks


requested_maxes = [1, 2, 5, 6, 9]
with ThreadPoolExecutor(max_workers=5) as pool:
    futs = [pool.submit(partial(issue_request, max)) for max in requested_maxes]
    chunks_list = [fut.result() for fut in futs]
    for max, chunks in zip(requested_maxes, chunks_list):
        assert chunks == [str(i) for i in range(max)]

# __batch_size_fn_begin__
from typing import List

from ray import serve
from ray.serve.handle import DeploymentHandle


class Graph:
    """Simple graph data structure for GNN workloads."""

    def __init__(self, num_nodes: int, node_features: list):
        self.num_nodes = num_nodes
        self.node_features = node_features


@serve.deployment
class GraphNeuralNetwork:
    @serve.batch(
        max_batch_size=10000,  # Maximum total nodes per batch
        batch_wait_timeout_s=0.1,
        batch_size_fn=lambda graphs: sum(g.num_nodes for g in graphs),
    )
    async def predict(self, graphs: List[Graph]) -> List[float]:
        """Process a batch of graphs, batching by total node count."""
        # The batch_size_fn ensures that the total number of nodes
        # across all graphs in the batch doesn't exceed max_batch_size.
        # This prevents GPU memory overflow.
        results = []
        for graph in graphs:
            # Your GNN model inference logic here
            # For this example, just return a simple score
            score = float(graph.num_nodes * 0.1)
            results.append(score)
        return results

    async def __call__(self, graph: Graph) -> float:
        return await self.predict(graph)


handle: DeploymentHandle = serve.run(GraphNeuralNetwork.bind())

# Create test graphs with varying node counts
graphs = [
    Graph(num_nodes=100, node_features=[1.0] * 100),
    Graph(num_nodes=5000, node_features=[2.0] * 5000),
    Graph(num_nodes=3000, node_features=[3.0] * 3000),
]

# Send requests - they'll be batched by total node count
results = [handle.remote(g).result() for g in graphs]
print(f"Results: {results}")
# __batch_size_fn_end__

# __batch_size_fn_nlp_begin__
from typing import List

from ray import serve
from ray.serve.handle import DeploymentHandle


@serve.deployment
class TokenBatcher:
    @serve.batch(
        max_batch_size=512,  # Maximum total tokens per batch
        batch_wait_timeout_s=0.1,
        batch_size_fn=lambda sequences: sum(len(s.split()) for s in sequences),
    )
    async def process(self, sequences: List[str]) -> List[int]:
        """Process text sequences, batching by total token count."""
        # The batch_size_fn ensures total tokens don't exceed max_batch_size.
        # This is useful for transformer models with fixed context windows.
        return [len(seq.split()) for seq in sequences]

    async def __call__(self, sequence: str) -> int:
        return await self.process(sequence)


handle: DeploymentHandle = serve.run(TokenBatcher.bind())

# Create sequences with different lengths
sequences = [
    "This is a short sentence",
    "This is a much longer sentence with many more words to process",
    "Short",
]

# Send requests - they'll be batched by total token count
results = [handle.remote(seq).result() for seq in sequences]
print(f"Token counts: {results}")
# __batch_size_fn_nlp_end__
