(kuberay-vllm-rayservice-example)=

# Serve a Large Language Model with vLLM on Kubernetes

This guide demonstrates how to Serve a Large Language Model with vLLM on Kubernetes using KubeRay. The example in this guide deploys the `meta-llama/Meta-Llama-3-8B-Instruct` model from Hugging Face on Google Kubernetes Engine (GKE).

## Prerequisites

This example downloads model weights from Hugging Face. You need to complete the following
prerequisites to successfully complete this guide:
* A [Hugging Face account](https://huggingface.co/)
* A Hugging Face [access token](https://huggingface.co/docs/hub/security-tokens) with read access to gated repos.
* Access to the Llama 3 8B model. Getting access usually requires signing an agreement on Hugging Face to access this model. Go to the [Llama 3 model page](https://huggingface.co/meta-llama/Meta-Llama-3-8B) for more details.

## Create a Kubernetes cluster on GKE

Create a GKE cluster with a GPU node pool:
```sh
gcloud container clusters create kuberay-gpu-cluster \
    --machine-type=g2-standard-24 \
    --location=us-east4-c \
    --num-nodes=2 \
    --accelerator=type=nvidia-l4,count=2,gpu-driver-version=latest
```

This example uses L4 GPUs. Each model replica uses 2 L4 GPUs using vLLM's tensor parallelism.

## Install the KubeRay Operator

Follow [Deploy a KubeRay operator](kuberay-operator-deploy) to install the latest stable KubeRay operator from the Helm repository.
The KubeRay operator Pod must be on the CPU node if you set up the taint for the GPU node pool correctly.

## Create a Kubernetes Secret containing your Hugging Face access token

Create a Kubernetes Secret containing your Hugging Face access token:
```sh
export HF_TOKEN=<Hugging Face access token>
kubectl create secret generic hf-secret   --from-literal=hf_api_token=${HF_TOKEN}   --dry-run=client -o yaml | kubectl apply -f -
```

This guide references this secret as an environment variable in the RayCluster used in the next steps.

## Deploy a RayService

Create a RayService custom resource:
```
kubectl apply -f https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/vllm/ray-service.vllm.yaml
```

This step configures RayService to deploy a Ray Serve app, running vLLM as the serving engine for the Llama 3 8B Instruct model. You can find the code for this example [on GitHub](https://github.com/ray-project/kuberay/blob/master/ray-operator/config/samples/vllm/serve.py).
You can inspect the Serve Config for more details about the Serve deployment:
```yaml
  serveConfigV2: |
    applications:
    - name: llm
      route_prefix: /
      import_path:  ray-operator.config.samples.vllm.serve:model
      deployments:
      - name: VLLMDeployment
        num_replicas: 1
        ray_actor_options:
          num_cpus: 8
          # NOTE: num_gpus is set automatically based on TENSOR_PARALLELISM
      runtime_env:
        working_dir: "https://github.com/ray-project/kuberay/archive/master.zip"
        pip: ["vllm==0.5.4"]
        env_vars:
          MODEL_ID: "meta-llama/Meta-Llama-3-8B-Instruct"
          TENSOR_PARALLELISM: "2"
```

Wait for the RayService resource to be ready. You can inspect its status by running the following command:
```
$ kubectl get rayservice llama-3-8b -o yaml
```

The output should contain the following:
```
status:
  activeServiceStatus:
    applicationStatuses:
      llm:
        healthLastUpdateTime: "2024-08-08T22:56:50Z"
        serveDeploymentStatuses:
          VLLMDeployment:
            healthLastUpdateTime: "2024-08-08T22:56:50Z"
            status: HEALTHY
        status: RUNNING
```

## Send a prompt

Confirm the Ray Serve deployment is healthy, then you can establish a port-forwarding session for the Serve app:

```sh
$ kubectl port-forward svc/llama-3-8b-serve-svc 8000
```

Note that KubeRay creates this Kubernetes Service after the Serve apps are ready and running.
This process may take several minutes after all Pods in the RayCluster are running.

Now you can send a prompt to the model:
```sh
$ curl http://localhost:8000/v1/chat/completions -H "Content-Type: application/json" -d '{
      "model": "meta-llama/Meta-Llama-3-8B-Instruct",
      "messages": [
        {"role": "system", "content": "You are a helpful assistant."},
        {"role": "user", "content": "Provide a brief sentence describing the Ray open-source project."}
      ],
      "temperature": 0.7
    }'
```

The output should be similar to the following, containing the generated response from the model:
```json
{"id":"cmpl-ce6585cd69ed47638b36ddc87930fded","object":"chat.completion","created":1723161873,"model":"meta-llama/Meta-Llama-3-8B-Instruct","choices":[{"index":0,"message":{"role":"assistant","content":"The Ray open-source project is a high-performance distributed computing framework that allows users to scale Python applications and machine learning models to thousands of nodes, supporting distributed data processing, distributed machine learning, and distributed analytics."},"logprobs":null,"finish_reason":"stop","stop_reason":null}],"usage":{"prompt_tokens":32,"total_tokens":74,"completion_tokens":42}}
```
