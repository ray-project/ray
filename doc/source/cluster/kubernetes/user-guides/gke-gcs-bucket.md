(kuberay-gke-bucket)=
# Configuring KubeRay to use Google Cloud Storage Buckets in GKE

If you are already familiar with Workload Identity in GKE, you can skip this document. The gist is that you need to specify a service account in each of the Ray pods after linking your Kubernetes service account to your Google Cloud service account. Otherwise, read on.

This example is an abridged version of the documentation at <https://cloud.google.com/kubernetes-engine/docs/how-to/workload-identity>. The full documentation is worth reading if you are interested in the details.

## Create a Kubernetes cluster on GKE

This example creates a minimal KubeRay cluster using GKE.

Run this and all following commands on your local machine or on the [Google Cloud Shell](https://cloud.google.com/shell). If running from your local machine, install the [Google Cloud SDK](https://cloud.google.com/sdk/docs/install).

```bash
gcloud container clusters create cloud-bucket-cluster \
    --num-nodes=1 --min-nodes 0 --max-nodes 1 --enable-autoscaling \
    --zone=us-west1-b --machine-type e2-standard-8 \
    --workload-pool=my-project-id.svc.id.goog # Replace my-project-id with your GCP project ID
```


This command creates a Kubernetes cluster named `cloud-bucket-cluster` with one node in the `us-west1-b` zone. This example uses the `e2-standard-8` machine type, which has 8 vCPUs and 32 GB RAM.

For more information on how to find your project ID, see <https://support.google.com/googleapi/answer/7014113?hl=en> or <https://cloud.google.com/resource-manager/docs/creating-managing-projects>.

Now get credentials for the cluster to use with `kubectl`:

```bash
gcloud container clusters get-credentials cloud-bucket-cluster --zone us-west1-b --project my-project-id
```

## Create an IAM Service Account

```bash
gcloud iam service-accounts create my-iam-sa
```

## Create a Kubernetes Service Account

```bash
kubectl create serviceaccount my-ksa
```

## Link the Kubernetes Service Account to the IAM Service Account and vice versa

In the following two commands, replace `default` with your namespace if you are not using the default namespace.

```bash
gcloud iam service-accounts add-iam-policy-binding my-iam-sa@my-project-id.iam.gserviceaccount.com \
    --role roles/iam.workloadIdentityUser \
    --member "serviceAccount:my-project-id.svc.id.goog[default/my-ksa]"
```

```bash
kubectl annotate serviceaccount my-ksa \
    --namespace default \
    iam.gke.io/gcp-service-account=my-iam-sa@my-project-id.iam.gserviceaccount.com
```

## Create a Google Cloud Storage Bucket and allow the Google Cloud Service Account to access it

Please follow the documentation at <https://cloud.google.com/storage/docs/creating-buckets> to create a bucket using the Google Cloud Console or the `gsutil` command line tool.

This example gives the principal `my-iam-sa@my-project-id.iam.gserviceaccount.com` "Storage Admin" permissions on the bucket. Enable the permissions in the Google Cloud Console ("Permissions" tab under "Buckets" > "Bucket Details") or with the following command:

```bash
gsutil iam ch serviceAccount:my-iam-sa@my-project-id.iam.gserviceaccount.com:roles/storage.admin gs://my-bucket
```

## Create a minimal RayCluster YAML manifest

You can download the RayCluster YAML manifest for this tutorial with `curl` as follows:

```bash
curl -LO https://raw.githubusercontent.com/ray-project/kuberay/v1.5.0/ray-operator/config/samples/ray-cluster.gke-bucket.yaml
```

The key parts are the following lines:

```yaml
      spec:
        serviceAccountName: my-ksa
        nodeSelector:
          iam.gke.io/gke-metadata-server-enabled: "true"
```

Include these lines in every pod spec of your Ray cluster. This example uses a single-node cluster (1 head node and 0 worker nodes) for simplicity.

## Create the RayCluster

```bash
kubectl apply -f ray-cluster.gke-bucket.yaml
```

## Test GCS bucket access from the RayCluster

Use `kubectl get pod` to get the name of the Ray head pod.  Then run the following command to get a shell in the Ray head pod:

```bash
kubectl exec -it raycluster-mini-head-xxxx -- /bin/bash
```

In the shell, run `pip install google-cloud-storage` to install the Google Cloud Storage Python client library.

(For production use cases, you will need to make sure `google-cloud-storage` is installed on every node of your cluster, or use `ray.init(runtime_env={"pip": ["google-cloud-storage"]})` to have the package installed as needed at runtime -- see <https://docs.ray.io/en/latest/ray-core/handling-dependencies.html#runtime-environments> for more details.)

Then run the following Python code to test access to the bucket:

```python
import ray
import os
from google.cloud import storage

GCP_GCS_BUCKET = "my-bucket"
GCP_GCS_FILE = "test_file.txt"

ray.init(address="auto")

@ray.remote
def check_gcs_read_write():
    client = storage.Client()
    bucket = client.get_bucket(GCP_GCS_BUCKET)
    blob = bucket.blob(GCP_GCS_FILE)

    # Write to the bucket
    blob.upload_from_string("Hello, Ray on GKE!")

    # Read from the bucket
    content = blob.download_as_text()

    return content

result = ray.get(check_gcs_read_write.remote())
print(result)
```

You should see the following output:

```text
Hello, Ray on GKE!
```
