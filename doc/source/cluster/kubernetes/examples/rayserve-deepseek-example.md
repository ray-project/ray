(kuberay-rayservice-deepseek-example)=

# Serve Deepseek R1 using Ray Serve LLM on GKE

This guide provides a step-by-step guide for deploying a Large Language Model (LLM) using Ray Serve LLM on Kubernetes. Leveraging KubeRay, Ray Serve, and vLLM, this guide deploys the  `deepseek-ai/DeepSeek-R1` model from Hugging Face, enabling scalable, efficient, and OpenAI-compatible LLM serving within a Kubernetes environment. See [Serving LLMs](serving_llms) for information on Ray Serve LLM.

## Prerequisites
As a large-scale model, DeepSeek requires deployment on nodes with GPU accelerators within a GKE cluster, such as the A3 High or A3 Mega machine types.

Before creating the cluster, please ensure that your project has sufficient quota for the required accelerators.

Afterward, refer to the [guide](https://cloud.google.com/cluster-toolkit/docs/deploy/deploy-a3-mega-cluster#create-reservation) (using A3 Mega as an example) to create a corresponding placement policy and reservation.

## Step 1: Create a Kubernetes cluster with accelerators
Please export all variables that would be used in cluster creation:

```sh
export CLUSTER_NAME = my_cluster # replace this with your own cluster name
export CLUSTER_LOCATION = australia-southeast1-c # The region or zone where your GKE cluster is located (e.g., us-central1 or us-central1-a).
export NODE_NUMBER = 2 # The number of worker nodes to create in the node pool, ideally should be >=2.
export PROJECT_ID = project_id # replace this with your project id
export NODEPOOL_NAME = my_nodepool_name # The name for the new node pool you are creating.
export RESERVATION_NAME = reservation_name # The name of the Compute Engine reservation to ensure capacity for your nodes.
export SERVICE_ACCOUNT = service_account # The IAM service account that the nodes will use for permissions.
export NODE_LOCATION = australia-southeast1-c # The specific zone where the nodes in this node pool will be created.
export PLACEMENT_POLICY_NAME = placement_policy # The name of the compact placement policy.
```
Use the following command to create a cluster with default system nodepools. (using A3 Mega as an example)

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
          "content": "who are you?"}
      ],
      "temperature": 0.7
    }'
```

The output should be in the following format:

```
{
  "id": "deepseek-35a3aca1-5186-4156-b43d-d07e3eee361e",
  "object": "chat.completion",
  "created": 1753293398,
  "model": "deepseek",
  "choices": [
    {
      "index": 0,
      "message": {
        "role": "assistant",
        "reasoning_content": null,
        "content": "Greetings! I'm DeepSeek-R1, an artificial intelligence assistant created by DeepSeek. I'm at your service and would be delighted to assist you with any inquiries or tasks you may have.\n</think>\n\nGreetings! I'm DeepSeek-R1, an artificial intelligence assistant created by DeepSeek. I'm at your service and would be delighted to assist you with any inquiries or tasks you may have.",
        "tool_calls": []
      },
      "logprobs": null,
      "finish_reason": "stop",
      "stop_reason": null
    }
  ],
  "usage": {
    "prompt_tokens": 10,
    "total_tokens": 97,
    "completion_tokens": 87,
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
