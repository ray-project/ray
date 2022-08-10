# __user_input_and_preprocessor_start__
import asyncio
from ray import serve
from ray.serve.dag import InputNode


@serve.deployment
async def preprocessor(input_data: str):
    """Simple feature processing that converts str to int"""
    await asyncio.sleep(0.1)  # Manual delay for blocking computation
    return int(input_data)


@serve.deployment
async def avg_preprocessor(input_data):
    """Simple feature processing that returns average of input list as float."""
    await asyncio.sleep(0.15)  # Manual delay for blocking computation
    return sum(input_data) / len(input_data)


# DAG building
with InputNode() as dag_input:
    # Partial access of user input by index
    preprocessed_1 = preprocessor.bind(dag_input[0])
    preprocessed_2 = avg_preprocessor.bind(dag_input[1])

# __user_input_and_preprocessor_end__


# __model_and_combiner_instantiate_start__
@serve.deployment
class Model:
    def __init__(self, weight: int):
        self.weight = weight

    async def forward(self, input: int):
        await asyncio.sleep(0.3)  # Manual delay for blocking computation
        return f"({self.weight} * {input})"


@serve.deployment
class Combiner:
    def __init__(self, m1: Model, m2: Model):
        self.m1 = m1
        self.m2 = m2

    async def run(self, req_part_1, req_part_2, operation):
        # Merge model input from two preprocessors
        req = f"({req_part_1} + {req_part_2})"

        # Submit to both m1 and m2 with same req data in parallel
        r1_ref = await self.m1.forward.remote(req)
        r2_ref = await self.m2.forward.remote(req)

        # Async gathering of model forward results for same request data
        await asyncio.gather(*[r1_ref, r2_ref])


# DAG building
with InputNode() as dag_input:
    # Partial access of user input by index
    preprocessed_1 = preprocessor.bind(dag_input[0])
    preprocessed_2 = avg_preprocessor.bind(dag_input[1])
    m1 = Model.bind(1)
    m2 = Model.bind(2)
    combiner = Combiner.bind(m1, m2)
    dag = combiner.run.bind(preprocessed_1, preprocessed_2, dag_input[2])
# __model_and_combiner_instantiate_end__


# __dynamic_aggregate_start__
@serve.deployment
class Combiner:
    ...

    async def run(self, req_part_1, req_part_2, operation):
        # Merge model input from two preprocessors
        req = f"({req_part_1} + {req_part_2})"

        # Submit to both m1 and m2 with same req data in parallel
        r1_ref = self.m1.forward.remote(req)
        r2_ref = self.m2.forward.remote(req)

        # Async gathering of model forward results for same request data
        rst = await asyncio.gather(*[r1_ref, r2_ref])

        # Control flow that determines runtime behavior based on user input
        if operation == "sum":
            return f"sum({rst})"
        else:
            return f"max({rst})"


# __dynamic_aggregate_end__

# __driver_to_http_ingress_start__
from ray.serve.drivers import DAGDriver  # noqa: E402
from ray.serve.http_adapters import json_request  # noqa: E402

# DAG building
with InputNode() as dag_input:
    ...
    dag = combiner.run.bind(
        preprocessed_1,
        preprocessed_2,
        dag_input[2],  # Partial access of user input by index
    )

    # Each serve dag has a driver deployment as ingress that can be user provided.
    serve_dag = DAGDriver.options(route_prefix="/my-dag", num_replicas=2).bind(
        dag, http_adapter=json_request
    )
# __driver_to_http_ingress_end__

# __full_e2e_code_start__
import time  # noqa: E402
import asyncio  # noqa: E402,F811
import requests  # noqa: E402

import ray  # noqa: E402
from ray import serve  # noqa: E402
from ray.serve.dag import InputNode  # noqa: E402
from ray.serve.drivers import DAGDriver  # noqa: E402
from ray.serve.http_adapters import json_request  # noqa: E402


@serve.deployment
async def preprocessor(input_data: str):
    """Simple feature processing that converts str to int"""
    await asyncio.sleep(0.1)  # Manual delay for blocking computation
    return int(input_data)


@serve.deployment
async def avg_preprocessor(input_data):
    """Simple feature processing that returns average of input list as float."""
    await asyncio.sleep(0.15)  # Manual delay for blocking computation
    return sum(input_data) / len(input_data)


@serve.deployment
class Model:
    def __init__(self, weight: int):
        self.weight = weight

    async def forward(self, input: int):
        await asyncio.sleep(0.3)  # Manual delay for blocking computation
        return f"({self.weight} * {input})"


@serve.deployment
class Combiner:
    def __init__(self, m1: Model, m2: Model):
        self.m1 = m1
        self.m2 = m2

    async def run(self, req_part_1, req_part_2, operation):
        # Merge model input from two preprocessors
        req = f"({req_part_1} + {req_part_2})"

        # Submit to both m1 and m2 with same req data in parallel
        r1_ref = await self.m1.forward.remote(req)
        r2_ref = await self.m2.forward.remote(req)

        # Async gathering of model forward results for same request data
        rst = await asyncio.gather(r1_ref, r2_ref)

        # Control flow that determines runtime behavior based on user input
        if operation == "sum":
            return f"sum({rst})"
        else:
            return f"max({rst})"


# DAG building
with InputNode() as dag_input:
    # Partial access of user input by index
    preprocessed_1 = preprocessor.bind(dag_input[0])
    preprocessed_2 = avg_preprocessor.bind(dag_input[1])
    # Multiple instantiation of the same class with different args
    m1 = Model.bind(1)
    m2 = Model.bind(2)
    # Use other DeploymentNode in bind()
    combiner = Combiner.bind(m1, m2)
    # Use output of function DeploymentNode in bind()
    dag = combiner.run.bind(preprocessed_1, preprocessed_2, dag_input[2])

    # Each serve dag has a driver deployment as ingress that can be user provided.
    serve_dag = DAGDriver.options(route_prefix="/my-dag", num_replicas=2).bind(
        dag, http_adapter=json_request
    )


dag_handle = serve.run(serve_dag)

# Warm up
ray.get(dag_handle.predict.remote(["0", [0, 0], "sum"]))

# Python handle
cur = time.time()
print(ray.get(dag_handle.predict.remote(["5", [1, 2], "sum"])))
print(f"Time spent: {round(time.time() - cur, 2)} secs.")
# Http endpoint
cur = time.time()
print(requests.post("http://127.0.0.1:8000/my-dag", json=["5", [1, 2], "sum"]).text)
print(f"Time spent: {round(time.time() - cur, 2)} secs.")

# Python handle
cur = time.time()
print(ray.get(dag_handle.predict.remote(["1", [0, 2], "max"])))
print(f"Time spent: {round(time.time() - cur, 2)} secs.")

# Http endpoint
cur = time.time()
print(requests.post("http://127.0.0.1:8000/my-dag", json=["1", [0, 2], "max"]).text)
print(f"Time spent: {round(time.time() - cur, 2)} secs.")

# __full_e2e_code_end__
