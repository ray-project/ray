(kuberay-tpu-multi-host-vllm)=

# Serve a LLM on GKE with Multi-Host TPUs

This guide showcases how to serve an LLM and run multi-host inference with vLLM and multi-host TPUs. A multi-host TPU slice is a node pool that contains two or more interconnected TPU VMs. Multi-host TPU slices enable users to run highly intensive workloads such as inference and training with large-scale AI models that don't fit on one VM host. For more information about TPUs, see [Use TPUs with KubeRay](kuberay-tpu). This example showcases serving and inference with Llama-3-70B.

## Step 1: Create a Kubernetes Cluster with TPUs and the Ray Operator Enabled

Follow [Creating a GKE Cluster with TPUs for KubeRay](kuberay-gke-tpu-cluster-setup) to create a GKE Autopilot or Standard cluster with 1 CPU node and a TPU nodepool. When creating a GKE Cluster, enable the Ray Addon to automatically install the KubeRay operator and TPU initialization webhook in-cluster. Multi-host TPU support with a RayService is available in GKE versions 1.30.5-gke.1014000+ and 1.31.1-gke.1104000+ for each respective minor version.

## Step 2: [Optional] Install the KubeRay operator

Skip this step if the [Ray Operator Addon](https://cloud.google.com/kubernetes-engine/docs/add-on/ray-on-gke/concepts/overview) is enabled in your GKE cluster. Follow [Deploy a KubeRay operator](kuberay-operator-deploy) instructions to install the latest stable KubeRay operator from the Helm repository. Multi-host TPU support is available in KubeRay v1.1.0+. Note that the YAML file in this example uses `serveConfigV2`, which KubeRay supports starting from v0.6.0.

## Step 3: Build a vLLM image with TPU dependencies

vLLM supports TPUs using PyTorch XLA, providing a [Dockerfile.tpu](https://github.com/vllm-project/vllm/blob/main/Dockerfile.tpu) for users to build their own vLLM image with TPU dependencies. This image includes ray[default] as a dependency and will serve as the image on our Ray head and workers.

Create a Docker repository to store the container images for this tutorial:
```sh
gcloud artifacts repositories create REPOSITORY \
  --repository-format=docker \
  --location=us-central1
```

Clone the vLLM repository:
```sh
git https://github.com/vllm-project/vllm.git
cd vllm
```

Build the TPU image:
```sh
docker build -f Dockerfile.tpu -t vllm-tpu .
```

Tag the image with your Artifact Registry name:
```sh
docker tag SOURCE-IMAGE LOCATION-docker.pkg.dev/PROJECT-ID/REPOSITORY/IMAGE:TAG
```
Replace SOURCE-IMAGE with the local image name or image ID and TAG with the tag. If you don't specify a tag, Docker applies the default latest tag.

Push the vLLM image to your Artifact registry:
```sh
docker push LOCATION-docker.pkg.dev/PROJECT-ID/REPOSITORY/IMAGE
```
## Step 4: Create a Kubernetes Secret for Hugging Face credentials

This example uses meta-llama/Meta-Llama-3-70B, a gated Hugging Face model that requires access to be granted before use. Create a Hugging Face account, if you don't already have one, and follow the steps on the [model page](https://huggingface.co/meta-llama/Meta-Llama-3-70B) to request access to the model. Save your Hugging Face token for the following steps.

Set HF_TOKEN environment variable:
```sh
export HF_TOKEN=HUGGING_FACE_TOKEN
```
Replace HUGGING_FACE_TOKEN with your Hugging Face access token.

Create a Kubernetes Secret with your Hugging Face credentials:
```sh
kubectl create secret generic hf-secret \
  --from-literal=hf_api_token=${HF_TOKEN} \
  --dry-run=client -o yaml | kubectl apply -f -
```

## Step 5: Install the RayService CR

Create a file named vllm-ray-serve-tpu.yaml with the following contents:
```sh
apiVersion: ray.io/v1
kind: RayService
metadata:
  name: vllm-tpu
spec:
  serveConfigV2: |
    applications:
      - name: llm
        import_path: ai-ml.gke-ray.rayserve.llm.serve_tpu:model
        deployments:
        - name: VLLMDeployment
          num_replicas: 1
        runtime_env:
          working_dir: "https://github.com/GoogleCloudPlatform/kubernetes-engine-samples/archive/main.zip"
        env_vars:
          MODEL_ID: "meta-llama/Meta-Llama-3-70B"
          TPU_CHIPS: 8
  rayClusterConfig:
    rayVersion: 2.34.0
    headGroupSpec:
      rayStartParams: {}
      template:
        spec:
          containers:
          - name: ray-head
            image: <SOURCE_IMAGE>
            imagePullPolicy: IfNotPresent
            ports:
            - containerPort: 6379
              name: gcs
            - containerPort: 8265
              name: dashboard
            - containerPort: 10001
              name: client
            - containerPort: 8000
              name: serve
            env:
            - name: HUGGING_FACE_HUB_TOKEN
              valueFrom:
                secretKeyRef:
                  name: hf-secret
                  key: hf_api_token
            resources:
              limits:
                cpu: "8"
                memory: 40G
              requests:
                cpu: "8"
                memory: 40G
    workerGroupSpecs:
    - groupName: tpu-group
      replicas: 1
      minReplicas: 0
      maxReplicas: 2
      numOfHosts: 2
      rayStartParams: {}
      template:
        spec:
          containers:
            - name: ray-worker
              image: <SOURCE_IMAGE>
              imagePullPolicy: IfNotPresent
              resources:
                limits:
                  cpu: "100"
                  google.com/tpu: "4"
                  memory: 200G
                requests:
                  cpu: "100"
                  google.com/tpu: "4"
                  memory: 200G
              env:
                - name: HUGGING_FACE_HUB_TOKEN
                  valueFrom:
                    secretKeyRef:
                      name: hf-secret
                      key: hf_api_token
              volumeMounts:
                - mountPath: /data
                  name: ephemeral-volume
          volumes:
            - name: ephemeral-volume
              ephemeral:
                volumeClaimTemplate:
                  metadata:
                    labels:
                      type: ephemeral
                  spec:
                    accessModes: ["ReadWriteOnce"]
                    storageClassName: "premium-rwo"
                    resources:
                      requests:
                        storage: 200Gi
          nodeSelector:
            cloud.google.com/gke-tpu-accelerator: tpu-v4-podslice
            cloud.google.com/gke-tpu-topology: 2x2x2
```
Replace `SOURCE-IMAGE` on the head and workers with the image name and tag pushed to Artifact Registry in the previous step.

Create the RayService CR:
```sh
kubectl apply -f vllm-ray-serve-tpu.yaml
```

## Step 6: View the Serve deployment in the Ray Dashboard

Verify that you deployed the RayService CR and it's running:

```sh
kubectl get rayservice

# NAME               SERVICE STATUS   NUM SERVE ENDPOINTS
# vllm-tpu-serve-svc   Running          2
```

Port-forward the Ray Dashboard from the Ray Serve service. To view the dashboard, open http://localhost:8265/ on your local machine.
```sh
kubectl port-forward svc/vllm-tpu-serve-svc 8265:8265 2>&1 >/dev/null &
```

## Step 7: Send prompts to the model server

Port-forward the model endpoint from Ray head:
```sh
kubectl port-forward svc/vllm-tpu-serve-svc 8000:8000 2>&1 >/dev/null &
```

Send a text prompt to the Llama model:
```sh
curl -X POST http://localhost:8000/v1/generate -H "Content-Type: application/json" -d '{"prompt": "What are the top 5 most popular programming languages? Be brief.", "max_tokens": 1024}'
```

Output should resemble the following:
```sh
{"prompt": "What are the top 5 most popular programming languages? Be brief.", "text": " The answer is based on the Tiobe Index, which is a programming language popularity index that has been ranking programming languages since 2001.\n\n1.  **Java**: 14.63% of the index.\n2.  **Python**: 11.52% of the index.\n3.  **C**: 7.94% of the index.\n4.  **C++**: 7.17% of the index.\n5.  **JavaScript**: 6.86% of the index.\n\nThe Tiobe Index is based on searches on search engines like Google, as well as on forums and other online platforms. It does not necessarily reflect the actual usage of programming languages in the industry, but it is a good indicator of their popularity and usage in the programming community.
...}
```
