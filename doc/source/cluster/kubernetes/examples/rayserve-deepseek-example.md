(kuberay-rayservice-deepseek-example)=

# Serve Deepseek R1 using Ray Serve LLM on GKE

This guide provides a step-by-step guide for deploying a Large Language Model (LLM) using Ray Serve LLM on Kubernetes. Leveraging KubeRay, Ray Serve, and vLLM, this guide deploys the  `deepseek-ai/DeepSeek-R1` model from Hugging Face, enabling scalable, efficient, and OpenAI-compatible LLM serving within a Kubernetes environment. See [Serving LLMs](serving_llms) for information on Ray Serve LLM.

## Prerequisites
As a large-scale model, DeepSeek requires deployment on nodes with GPU accelerators within a GKE cluster, such as the A3 High or A3 Mega machine types.

Before creating the cluster, please ensure that your project has sufficient quota for the required accelerators.

Afterward, refer to the [guide](https://cloud.google.com/cluster-toolkit/docs/deploy/deploy-a3-mega-cluster#create-reservation) (using A3 Mega as an example) to create a corresponding placement policy and reservation.

## Step 1: Create a Kubernetes cluster with accelerators
Please use the following command to create a cluster with default system nodepools. (using A3 Mega as an example)

```sh
$ gcloud container clusters create ${CLUSTER_NAME} \
    --addons GcsFuseCsiDriver \
    --location=${CLUSTER_LOCATION} \
    --machine-type=e2-standard-16 \
    --release-channel=rapid \
    --cluster-version=1.32 \
    --num-nodes=${NODE_NUMBER} \
    --workload-pool=${PROJECT_ID}.svc.id.goog \ 
    --enable-image-streaming
```

```sh
$ gcloud beta container node-pools create ${NODEPOOL_NAME} \
    --cluster ${CLUSTER_NAME} \ 
    --machine-type a3-megagpu-8g \
    --reservation ${RESERVATION_NAME} \
    --reservation-affinity specific \
    --num-nodes ${NODE_NUMBER} \
    --accelerator "type=nvidia-h100-mega-80gb,count=8" \
    --service-account ${SERVICE_ACCOUNT} \
    --project ${PROJECT_ID} \
    --location ${CLUSTER_LOCATION} \
    --node-locations ${NODE_LOCATION} \
    --host-maintenance-interval=PERIODIC \
    --placement-policy ${PLACEMENT_POLICY_NAME}
```

## Step 2: Install the KubeRay operator

Connect to the cluster, then install the most recent stable KubeRay operator from the Helm repository by following [Deploy a KubeRay operator](../getting-started/kuberay-operator-installation.md). The Kubernetes `NoSchedule` taint in the example config prevents the KubeRay operator pod from running on a GPU node.

## Step 3: Deploy a RayService

Deploy the Ray Serve deepseek service config .yaml file using the following command:

```sh
$ kubectl apply -f https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/ray-service.deepseek.yaml
```

This step sets up a custom Ray Serve app to serve the `deepseek-ai/DeepSeek-R1` model on 2 worker nodes. You can inspect and modify the `serveConfigV2` section in the YAML file to learn more about the Serve app:
```yaml
serveConfigV2: |
  applications:
  - args:
      llm_configs:
        - model_loading_config:
            model_id: "deepseek"
            model_source: "deepseek-ai/DeepSeek-R1"
          accelerator_type: "H100"
          deployment_config:
            autoscaling_config:
              min_replicas: 1
              max_replicas: 1
          runtime_env:
            env_vars:
              VLLM_USE_V1: "1"
          engine_kwargs:
            tensor_parallel_size: 8
            pipeline_parallel_size: 2
            gpu_memory_utilization: 0.92
            dtype: "auto"
            max_num_seqs: 40
            max_model_len: 16384
            enable_chunked_prefill: true
            enable_prefix_caching: true
    import_path: ray.serve.llm:build_openai_app
    name: llm_app
    route_prefix: "/"
```

In particular, this configuration loads the model from `deepseek-ai/DeepSeek-R1` and sets its `model_id` to `deepseek`. The `LLMDeployment` initializes the underlying LLM engine using the `engine_kwargs` field, which includes key performance tuning parameters:

- `tensor_parallel_size: 8`

  This setting enables tensor parallelism, splitting individual large layers of the model across 8 GPUs. This variable should be adjusted according to the number of GPUs used by cluster nodes.

- `pipeline_parallel_size: 2`
  
  This setting enables pipeline parallelism, dividing the model's entire set of layers into 2 sequential stages. This variable should be adjusted according to the cluster worker node numbers.


The `deployment_config` section sets the desired number of engine replicas. See [Serving LLMs](serving_llms) and the [Ray Serve config documentation](serve-in-production-config-file) for more information.

Wait for the RayService resource to become healthy. You can confirm its status by running the following command:
```sh
$ kubectl get rayservice deepseek-r1 -o yaml
```

After a few minutes, the result should be similar to the following:
```
status:
  activeServiceStatus:
    applicationStatuses:
      llm_app:
        serveDeploymentStatuses:
          LLMDeployment:deepseek:
            status: HEALTHY
          LLMRouter:
            status: HEALTHY
        status: RUNNING
```

## Step 4: Send a request

To send requests to the Ray Serve deployment, port-forward port 8000 from the Serve app service:
```sh
$ kubectl port-forward svc/deepseek-r1-serve-svc 8000
```

Note that this Kubernetes service comes up only after Ray Serve apps are running and ready.

Test the service with the following command:
```sh
$ curl http://localhost:8000/v1/chat/completions     -H "Content-Type: application/json"     -d '{
      "model": "deepseek",
      "messages": [
        {
          "role": "user", 
          "content": "Provide steps to serve an LLM using Ray Serve."}
      ],
      "temperature": 0.7
    }'
```

The output should be in the following format:

```
{
  "id": "deepseek-2d37b533-94a4-46dd-95ff-58aafc6a00e9",
  "object": "chat.completion",
  "created": 1753254398,
  "model": "deepseek",
  "choices": [
    {
      "index": 0,
      "message": {
        "role": "assistant",
        "reasoning_content": null,
        "content": "Okay, so I need to figure out how to serve a Large Language Model (LLM) using Ray Serve. Let me start by recalling what I know about Ray and Ray Serve. Ray is a framework for scaling AI and Python applications. Ray Serve is a scalable model-serving library built on top of Ray, designed to deploy machine learning models in production.\n\nFirst, I think the initial step would be to install the necessary packages. I remember that Ray and Ray Serve are separate packages, but maybe Ray Serve comes with Ray now? I should check that. Also, if I'm using a specific LLM like Hugging Face's Transformers, I might need to install those libraries too. So, installing ray with the serve component, maybe using pip install 'ray[serve]' and then transformers, torch, etc.\n\nNext, I need to create a Ray Serve deployment. From what I remember, Ray Serve uses a decorator-based approach where you define a class with an __init__ method to load the model and a __call__ method to handle requests. So, the steps would involve importing the necessary modules, starting the Ray cluster and Ray Serve instance, defining the deployment class, and then deploying it.\n\nWait, I should start by initializing Ray, right? So in the code, the first thing would be import ray and ray serve, then ray.init() to start a local cluster. Then, using serve.start() to start the Ray Serve instance.\n\nFor the model itself, I need to load the LLM. Let's say I'm using the Hugging Face pipeline. So in the deployment class's __init__ method, I would load the pipeline, like model = pipeline('text-generation', model_name='gpt2'). Then, in the __call__ method, when a request comes in, it would take the input text, pass it to the model, and return the generated text.\n\nI should make sure that the __call__ method is async or handles concurrent requests properly. Wait, Ray Serve handles the scaling by using multiple replicas, so maybe the __call__ method can be synchronous, and Ray will manage the concurrency with multiple workers. Or maybe I need to use async def __call__ if I want to handle multiple requests in a single replica. Hmm, I'm not sure. Maybe starting with a synchronous method and see.\n\nOnce the deployment is defined, I need to bind it and deploy it. The syntax for deploying has changed a bit. Previously, you used to have to create a deployment with a class and then deploy it. Now, maybe using a decorator like @serve.deployment on the class, then using serve.run(MyDeployment.bind()) to deploy it.\n\nAfter deploying, I can send requests to the model via HTTP or Python. The HTTP endpoint would be accessible at a certain port, like 8000 by default. So using requests.post(\"http://localhost:8000/\", json={\"text\": \"prompt here\"}) would send a request and get the response.\n\nScaling considerations: maybe setting the number of replicas in the deployment configuration. Like using @serve.deployment(num_replicas=4) to have multiple replicas handling requests, which would allow for higher throughput.\n\nAlso, for hardware acceleration, if using GPUs, I need to specify runtime environments or resource requirements in the deployment. Like setting ray_actor_options={\"num_gpus\": 1} in the decorator to allocate a GPU per replica.\n\nLet me outline the steps:\n\n1. Install Ray Serve and required ML libraries.\n2. Import Ray and Ray Serve, start Ray and Serve.\n3. Define the deployment class with __init__ (loading model) and __call__ (processing requests).\n4. Deploy the model using the deployment class.\n5. Query the model via HTTP or Python.\n6. Scaling and resource configuration (optional steps).\n\nWait, sometimes you might need to handle batching. If the model benefits from processing multiple inputs at once, you can enable batching. In Ray Serve, there's an @serve.batch decorator to batch requests. So, modifying the __call__ method to handle a list of requests and process them in a batch could improve efficiency.\n\nAlso, error handling is important. What if the model fails to load? Or if the input is malformed? The __call__ method should include try-except blocks to catch exceptions and return appropriate error messages.\n\nTesting is another step. After deploying, you can write a small script to send requests and verify the responses.\n\nDocumentation mentions that in Ray Serve, each deployment can have multiple routes, but by default, the route is based on the deployment name. So when you deploy MyDeployment, you can access it via /MyDeployment. Alternatively, you can set a custom route when binding.\n\nAnother thing: environment setup. If there are specific dependencies needed for the model (like transformers, torch), you might need to include them in a runtime environment, especially if deploying on a cluster. But for local deployment, installing them in the same environment should suffice.\n\nPutting this all together, the step-by-step would be:\n\n- Install dependencies: pip install 'ray[serve]' transformers torch\n- Write a script that:\n\n  - Imports ray and serve, starts ray.init(), then serve.start()\n  - Defines a deployment class with @serve.deployment, loads the model in __init__, processes requests in __call__\n  - Deploys it with serve.run()\n  - Optionally, tests it with a HTTP request or Python client\n\nWait, also, when starting the serve instance, in newer versions, maybe you don't need serve.start() if you use serve.run(), which handles starting the instance. Let me check the latest Ray Serve documentation.\n\nLooking at the latest Ray Serve docs (as of 2023), the recommended way is to use serve.run() which starts the instance if it's not already running. So the code structure would be:\n\nimport ray\nfrom ray import serve\n\n@serve.deployment\nclass MyDeployment:\n    def __init__(self):\n        # load model\n        pass\n\n    def __call__(self, request):\n        # process request\n        pass\n\nserve.run(MyDeployment.bind())\n\nSo that's the basic structure. Then, for the model, using Hugging Face pipeline as an example.\n\nIncluding error handling: maybe adding a pydantic model for request validation, but that's optional.\n\nAlso, for the HTTP request, the input needs to be parsed correctly. The __call__ method receives a Starlette request object, so you need to extract the JSON data from it.\n\nWait, in the __call__ method, the input is a Starlette request. So inside the method, you would do something like:\n\nasync def __call__(self, request: Request):\n    data = await request.json()\n    text = data[\"text\"]\n    # process text\n    return result\n\nBut if using async def, then the model's inference might block the event loop unless it's also async. Alternatively, for synchronous code, just use def __call__ and Ray will handle it in a thread pool.\n\nSo, for a synchronous model inference (like most Hugging Face models which are synchronous), using def __call__ is better, as async might lead to blocking. Therefore, inside the __call__ method:\n\ndef __call__(self, request: Request):\n    data = request.json()\n    text = data[\"text\"]\n    result = self.model(text, ...)\n    return result\n\nWait, actually, the request.json() is an async method, so if using def __call__, it can't await it. Therefore, the __call__ method needs to be async if you need to await request.json(). So to handle that, perhaps better to use async def __call__ and await the request.json(), but then if the model is synchronous, the inference will block the event loop. That's a problem.\n\nTherefore, maybe in that case, to avoid blocking, you should use a synchronous deployment with def __call__, and make sure to read the request body in a synchronous way. Alternatively, read the request body synchronously using data = await request.json() but that's not possible in a synchronous function.\n\nWait, perhaps the correct approach is to use async def and then run the blocking model inference in a separate thread using asyncio.to_thread() or a similar method. Or, use a synchronous __call__ method and use a thread pool executor to handle the requests. Hmm, this is getting complicated.\n\nAlternatively, maybe Ray Serve handles this by running each __call__ in a separate thread, so even if it's a synchronous function, it won't block the event loop. Or perhaps the default deployment uses a synchronous handler and handles concurrency via multiple threads.\n\nI need to check Ray Serve's documentation on synchronous vs asynchronous handlers. According to the docs, if you define an async def __call__ method, it runs in an asyncio event loop, allowing you to handle multiple requests concurrently in a single replica using async IO. But if the __call__ is synchronous (def), then each request is handled in a separate thread, allowing CPU-bound work without blocking the event loop.\n\nIn the case of model inference, which is CPU or GPU-bound and synchronous (like PyTorch models), using a synchronous __call__ is better, as it will be handled in a thread per request. So, in the deployment class, use def __call__ and inside, read the request data. However, accessing request.json() is an async method, which can't be called synchronously. Therefore, how do you get the data?\n\nAh, here's an issue. If the __call__ method is synchronous, you can't await request.json(). Therefore, you have to read the body synchronously. Wait, but the request object's .json() method is async. So perhaps in a synchronous function, you can't call it. Therefore, you would have to read the body as bytes first, then parse it.\n\nAlternatively, use async def __call__ and then run the model in a separate thread to avoid blocking. Let's see:\n\nasync def __call__(self, request: Request):\n    data = await request.json()\n    text = data[\"text\"]\n    # Run the synchronous model in a thread pool\n    result = await asyncio.get_event_loop().run_in_executor(None, self.model, text)\n    return result\n\nBut this requires importing asyncio and managing the executor, which might complicate things. However, this allows the async handler to not block the event loop.\n\nAlternatively, in Ray Serve, you can configure the deployment to handle parallelism. For example, setting @serve.deployment(num_replicas=4, ray_actor_options={\"num_cpus\": 1}) to have multiple replicas, each handling requests in separate processes. Then, each replica can handle requests synchronously, and since they are in separate processes, the GIL is less of an issue. But for high throughput, using async might be better.\n\nThis is getting a bit complicated. Maybe for simplicity, the answer can outline the basic steps using a synchronous __call__ and a way to read the request data without async. Wait, maybe using Starlette's request.body() and then parsing the JSON manually. Let's check.\n\nIn Starlette, you can get the request body as bytes with body = await request.body(), then parse it with json.loads(body). But again, that's an async operation. Therefore, in a synchronous __call__, you can't do that. Therefore, the __call__ must be async, and then the model's inference needs to be wrapped in a thread to avoid blocking.\n\nAlternatively, maybe the code example in Ray Serve's documentation shows how to handle this. Let me check a sample from Ray Serve's docs.\n\nFor example, in the Ray Serve documentation, they have an example:\n\nfrom ray import serve\nfrom fastapi import Request\n\n@serve.deployment\nclass MyModel:\n    def __init__(self):\n        # Load model\n        pass\n\n    async def __call__(self, request: Request):\n        data = await request.json()\n        return self.model(data)\n\nIn this case, the __call__ is async, and data is obtained via await request.json(). Then, the model's inference is done in the async method. If the model is blocking (synchronous), this would block the event loop. Therefore, to prevent that, they recommend using a thread pool executor.\n\nSo the correct approach would be:\n\nasync def __call__(self, request: Request):\n    data = await request.json()\n    # Run synchronous model in a thread pool\n    result = await asyncio.get_event_loop().run_in_executor(\n        None, self.model.generate, data[\"text\"]\n    )\n    return {\"result\": result}\n\nThis way, the blocking model call is offloaded to a thread, allowing the async handler to handle other requests while waiting for the model.\n\nBut this requires importing asyncio and structuring the code accordingly.\n\nTherefore, putting all together:\n\nInstallation:\n\npip install 'ray[serve]' transformers torch\n\nSample code:\n\nimport ray\nfrom ray import serve\nfrom fastapi import Request\nimport asyncio\nfrom transformers import pipeline\n\n@serve.deployment\nclass LLMServer:\n    def __init__(self):\n        # Load the LLM\n        self.model = pipeline(\"text-generation\", model=\"gpt2\")\n\n    async def __call__(self, request: Request):\n        # Parse request data\n        data = await request.json()\n        text = data.get(\"text\", \"\")\n        # Run model inference in a thread pool to avoid blocking\n        result = await asyncio.get_event_loop().run_in_executor(\n            None, self.model, text, {\"max_length\": 50}\n        )\n        return {\"response\": result[0][\"generated_text\"]}\n\n# Start Ray and Serve\nray.init()\nserve.run(LLMServer.bind())\n\n# Then you can query via HTTP or Python client\n\nThen, querying using Python:\n\nimport requests\n\nresponse = requests.post(\"http://localhost:8000/\", json={\"text\": \"Once upon a time\"})\nprint(response.json())\n\nAlternatively, using the Ray Serve handle:\n\nhandle = serve.get_app_handle(\"default\")\nresult = handle.remote({\"text\": \"Hello\"})\nprint(ray.get(result))\n\nWait, but in the deployment code, the route might be based on the deployment name. In this case, the deployment is LLMServer, so the route would be /LLMServer. Or, if you set the route name explicitly, you can change it.\n\nAlternatively, when binding, you can specify the route, but in recent versions of Ray Serve, the routing is managed differently. By default, when you deploy a deployment without an application, it's part of the default application, and the deployments are accessible under their names.\n\nBut in the example above, when using serve.run(LLMServer.bind()), the deployment is named LLMServer, so the HTTP endpoint would be at /LLMServer. Therefore, the POST request should be to http://localhost:8000/LLMServer.\n\nAlternatively, you can set the route prefix when creating the deployment. For example:\n\n@serve.deployment(route_prefix=\"/generate\")\nclass LLMServer:\n    ...\n\nThen, the endpoint would be /generate.\n\nAlternatively, when using FastAPI integration, you can compose multiple deployments into an app with multiple routes.\n\nBut for simplicity, maybe the example uses the default route based on the deployment name or sets a route_prefix.\n\nTherefore, adjusting the code to set the route_prefix:\n\n@serve.deployment(route_prefix=\"/\")\nclass LLMServer:\n    ...\n\nThen, the endpoint would be at the root URL.\n\nPutting that into the code:\n\n@serve.deployment(route_prefix=\"/\")\nclass LLMServer:\n    ...\n\nThen, the POST request is to http://localhost:8000/.\n\nAnother thing: when starting Ray, sometimes you might need to specify the runtime environment or num_gpus. For GPU support, you can set ray.init(num_gpus=1) or in the deployment's ray_actor_options.\n\nFor example, to have each replica use a GPU:\n\n@serve.deployment(ray_actor_options={\"num_gpus\": 1})\nclass LLMServer:\n    ...\n\nThis requires that the system has GPUs available.\n\nAdditionally, for scaling, setting the number of replicas:\n\n@serve.deployment(num_replicas=4, ray_actor_options={\"num_gpus\": 1})\nclass LLMServer:\n    ...\n\nThis would create 4 replicas, each using a GPU.\n\nNow, what about batch processing? To handle multiple requests in a batch, you can use the @serve.batch decorator. For example:\n\nfrom ray.serve import batch\n\n@serve.deployment\nclass BatchedLLM:\n    def __init__(self):\n        self.model = pipeline(...)\n\n    @batch(max_batch_size=8, batch_wait_timeout_s=0.1)\n    async def handle_batch(self, texts):\n        return self.model(texts)\n\n    async def __call__(self, request: Request):\n        data = await request.json()\n        text = data[\"text\"]\n        return await self.handle_batch(text)\n\nBut integrating this would complicate the example. For simplicity, the answer might mention batch processing as an advanced step.\n\nPutting all together, the steps are:\n\n1. Install Ray Serve and dependencies.\n2. Import and initialize Ray and Serve.\n3. Define the deployment class with model loading and request handling.\n4. Deploy the model.\n5. Query the model via HTTP or Python client.\n6. (Optional) Configure scaling, GPU usage, and batch processing.\n\nPotential mistakes I might have made:\n\n- Incorrect handling of async/synchronous code in the __call__ method.\n- Forgetting to start Ray and Serve.\n- Not setting the correct route_prefix, leading to 404 errors.\n- Not handling the request parsing correctly (e.g., using request.json() in a synchronous method).\n- Not including necessary parameters for the model pipeline, like max_length.\n\nTesting the code would help catch these issues. For example, if the model isn't loaded properly, the deployment would fail. If the request parsing is incorrect, the server returns errors.\n\nAnother consideration is model size. If the LLM is very large, loading it into each replica might consume a lot of memory. Using shared resources or model parallelism might be necessary, but that's beyond the basic setup.\n\nIn conclusion, the key steps involve setting up the deployment class correctly, handling async request parsing, deploying with Ray Serve, and querying the endpoint. Advanced configurations can be added as needed.\n</think>\n\nTo serve a Large Language Model (LLM) using Ray Serve, follow these steps:\n\n### 1. **Install Required Packages**\n```bash\npip install 'ray[serve]' transformers torch\n```\n\n### 2. **Import Libraries and Initialize Ray Serve**\n```python\nimport ray\nfrom ray import serve\nfrom fastapi import Request\nimport asyncio\nfrom transformers import pipeline\n```\n\n### 3. **Define the Deployment Class**\nCreate a class to load the model and handle requests. Use async to avoid blocking the event loop.\n\n```python\n@serve.deployment(route_prefix=\"/\")\nclass LLMServer:\n    def __init__(self):\n        # Load the model (e.g., Hugging Face pipeline)\n        self.model = pipeline(\"text-generation\", model=\"gpt2\")\n\n    async def __call__(self, request: Request):\n        # Parse the request\n        data = await request.json()\n        text = data.get(\"text\", \"\")\n\n        # Run synchronous model inference in a thread pool\n        result = await asyncio.get_event_loop().run_in_executor(\n            None,  # Uses default executor\n            lambda: self.model(text, max_length=50)[0][\"generated_text\"]\n        )\n        return {\"response\": result}\n```\n\n### 4. **Deploy the Model**\nStart Ray and deploy the model.\n\n```python\nray.init()\nserve.run(LLMServer.bind())\n```\n\n### 5. **Query the Model**\nSend HTTP requests or use the Ray Serve client.\n\n**Using Python `requests`:**\n```python\nimport requests\n\nresponse = requests.post(\"http://localhost:8000/\", json={\"text\": \"Once upon a time\"})\nprint(response.json())\n```\n\n**Using Ray Serve Handle:**\n```python\nhandle = serve.get_app_handle(\"default\")\nresult = ray.get(handle.remote({\"text\": \"Hello\"}))\nprint(result)\n```\n\n### 6. **(Optional) Advanced Configurations**\n- **GPU Support:** Allocate GPUs to replicas.\n  ```python\n  @serve.deployment(ray_actor_options={\"num_gpus\": 1})\n  class LLMServer:\n      ...\n  ```\n\n- **Scaling:** Increase replicas for higher throughput.\n  ```python\n  @serve.deployment(num_replicas=4)\n  class LLMServer:\n      ...\n  ```\n\n- **Batching:** Process multiple requests simultaneously.\n  ```python\n  from ray.serve import batch\n\n  @serve.deployment\n  class BatchedLLM:\n      def __init__(self):\n          self.model = pipeline(...)\n\n      @batch(max_batch_size=8, batch_wait_timeout_s=0.1)\n      async def handle_batch(self, texts):\n          return [self.model(text) for text in texts]\n\n      async def __call__(self, request: Request):\n          data = await request.json()\n          return await self.handle_batch(data[\"text\"])\n  ```\n\n### Summary\n- **Deployment:** Wrap the model in a Ray Serve class and deploy it.\n- **Async Handling:** Ensure non-blocking I/O for concurrent requests.\n- **Scalability:** Use replicas and batching for performance.\n- **Hardware:** Configure GPU usage as needed.\n\nThis setup allows you to serve an LLM efficiently with Ray Serve, enabling scalability and production-ready deployment.",
        "tool_calls": []
      },
      "logprobs": null,
      "finish_reason": "stop",
      "stop_reason": null
    }
  ],
  "usage": {
    "prompt_tokens": 17,
    "total_tokens": 4577,
    "completion_tokens": 4560,
    "prompt_tokens_details": null
  },
  "prompt_logprobs": null
}
```


## Step 5: View the Ray dashboard


```sh
$ kubectl get service
$ SERVICE_NAME=$(kubectl get service -n default | grep "deepseek-r1-raycluster-.*-head-svc" | awk '{print $1}')
$ kubectl port-forward svc/${SERVICE_NAME} 8265:8265
```

Once forwarded, navigate to the Serve tab on the dashboard to review application status, deployments, routers, logs, and other relevant features.
![LLM Serve Application](../images/ray_dashboard_deepseek.png)
