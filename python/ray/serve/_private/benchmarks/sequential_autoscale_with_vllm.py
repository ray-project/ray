import atexit
import logging
import os

import httpx
from starlette.requests import Request
from starlette.responses import Response

import docker
from docker.models.containers import Container
from docker.types import DeviceRequest

import ray
from ray import serve
from ray.serve import get_replica_context

logger = logging.getLogger("ray_serve_vllm")
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())


def start_vllm_container(
    replica_id: str,
    model_path: str,
    gpu_device_id: str,
) -> tuple[Container, int]:
    """
    Start vLLM in Docker, binding container:8000 to a random host port.
    Returns (container_obj, host_port).
    """
    name = f"ray_vllm_{replica_id}"
    image = "vllm/vllm-openai:latest"

    try:
        # Tell Docker “bind container port 8000 → random free host port”
        container = docker.from_env().containers.run(
            image=image,
            command=[
                "--model",
                "/models/my-model",
                "--gpu-memory-utilization",
                "0.9",
                "--max-model-len=2048",
            ],
            name=name,
            runtime="nvidia",
            device_requests=[
                DeviceRequest(
                    driver="nvidia", device_ids=[gpu_device_id], capabilities=[["gpu"]]
                )
            ],
            volumes={model_path: {"bind": "/models/my-model", "mode": "ro"}},
            ports={"8000/tcp": None},  # None → Docker chooses free host port
            detach=True,
        )
    except Exception as ex:
        logger.error(f"Error from docker:{str(ex)}")

    # reload so Docker assigns host port
    container.reload()
    port_info = container.attrs["NetworkSettings"]["Ports"]["8000/tcp"][0]
    host_port = int(port_info["HostPort"])

    logger.info(f"{name} → container:8000 mapped to host:{host_port}")
    return container, host_port


def stop_vllm_container(container_name: str):
    """
    Stop & remove the Docker container if it exists.
    """
    try:
        c = docker.from_env().containers.get(container_name)
        c.stop()
        c.remove()
        logger.info(f"Stopped & removed container {container_name}")
    except docker.errors.NotFound:
        logger.warning(f"Container {container_name} already gone")


@serve.deployment(
    ray_actor_options={"num_cpus": 0.5, "num_gpus": 1},
    autoscaling_config={
        "min_replicas": 1,
        "max_replicas": 5,
        "target_num_ongoing_requests_per_replica": 2,
        "upscale_delay_s": 2,
        "downscale_delay_s": 10,
    },
)
class VLLMService1:
    def __init__(self, model_path: str):
        ctx = get_replica_context()
        self.replica_id = ctx.replica_tag
        self.model_path = model_path

        cuda_env = os.environ.get("CUDA_VISIBLE_DEVICES", "0")
        gpu_id = cuda_env.split(",")[0]

        # start vLLM container + get its host port
        self.container, self.host_port = start_vllm_container(
            replica_id=self.replica_id,
            model_path=self.model_path,
            gpu_device_id=gpu_id,
        )

        # ensure container is torn down if the actor dies
        atexit.register(lambda: stop_vllm_container(self.container.name))

    async def __call__(self, request: dict) -> Response:
        """
        Proxy *any* incoming request to the vLLM container.
        Preserves method, path, query, headers, and body.
        """
        # 1) Reconstruct the target URL
        path = request["path"]  # e.g. "/v1/chat/completions"
        query = request["query"]  # e.g. "limit=10"
        url = f"http://localhost:{self.host_port}{path}"
        if query:
            url += f"?{query}"

        # 2) Read body & relevant headers
        body = request["body"]
        headers = {}
        if "content-type" in request["headers"]:
            headers["content-type"] = request["headers"]["content-type"]

        logger.error(f"[CALL: calling vllm with {body} on {url}]")

        # 3) Dispatch the request
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.request(
                    method=request["method"],
                    url=url,
                    headers=headers,
                    content=body,
                    timeout=None,
                )

            logger.error(f"[Reponse for {self.replica_id} : {resp.content}]")
        except Exception as ex:
            print(str(ex))

        # 4) Return raw response
        return Response(
            content=resp.content,
            status_code=resp.status_code,
            headers=resp.headers,
        )


@serve.deployment(
    ray_actor_options={"num_cpus": 0.5, "num_gpus": 1},
    autoscaling_config={
        "min_replicas": 1,
        "max_replicas": 5,
        "target_num_ongoing_requests_per_replica": 2,
        "upscale_delay_s": 2,
        "downscale_delay_s": 10,
    },
)
class VLLMService2:
    def __init__(self, model_path: str, ref=None):
        ctx = get_replica_context()
        self.replica_id = ctx.replica_tag
        self.model_path = model_path
        self.ref = ref

        cuda_env = os.environ.get("CUDA_VISIBLE_DEVICES", "0")
        gpu_id = cuda_env.split(",")[0]

        # start vLLM container + get its host port
        self.container, self.host_port = start_vllm_container(
            replica_id=self.replica_id,
            model_path=self.model_path,
            gpu_device_id=gpu_id,
        )

        # ensure container is torn down if the actor dies
        atexit.register(lambda: stop_vllm_container(self.container.name))

    async def __call__(self, request: Request) -> Response:
        """
        Proxy *any* incoming request to the vLLM container.
        Preserves method, path, query, headers, and body.
        """
        # 1) Reconstruct the target URL
        path = request.url.path  # e.g. "/v1/chat/completions"
        query = request.url.query  # e.g. "limit=10"
        url = f"http://localhost:{self.host_port}{path}"
        if query:
            url += f"?{query}"

        # 2) Read body & relevant headers
        body = await request.body()
        headers = {}
        if "content-type" in request.headers:
            headers["content-type"] = request.headers["content-type"]

        logger.error(f"[CALL: calling vllm with {body} on {url}]")

        # 3) Dispatch the request
        async with httpx.AsyncClient() as client:
            resp = await client.request(
                method=request.method,
                url=url,
                headers=headers,
                content=body,
                timeout=None,
            )

        if self.ref is not None:
            # Extract serializable request info
            request_data = {
                "method": request.method,
                "path": request.url.path,
                "query": request.url.query,
                "headers": dict(request.headers),
                "body": body.decode("utf-8", errors="ignore"),  # or base64 if binary
            }
            await self.ref.remote(request_data)

        logger.error(f"[Reponse for {self.replica_id} : {resp.content}]")

        # 4) Return raw response
        return Response(
            content=resp.content,
            status_code=resp.status_code,
            headers=resp.headers,
        )


if __name__ == "__main__":
    ray.init(
        address=os.environ.get("RAY_ADDRESS", "localhost:6980"),
        _temp_dir="/home/arthur/ray",
    )
    serve.start(detached=True, http_options={"host": "0.0.0.0", "port": 8787})

    model_path = os.environ.get("VLLM_MODEL_PATH", "/home/original_models/Qwen3-8B")

    vllm1 = VLLMService1.bind(model_path=model_path)

    vllm2 = VLLMService2.bind(model_path=model_path, ref=vllm1)

    serve.run(vllm2, route_prefix="/", blocking=True)
