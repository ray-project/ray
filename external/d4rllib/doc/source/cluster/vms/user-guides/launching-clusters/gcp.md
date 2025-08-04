# Launching Ray Clusters on GCP

This guide details the steps needed to start a Ray cluster in GCP.

To start a GCP Ray cluster, you will use the Ray cluster launcher with the Google API client.

## Install Ray cluster launcher

The Ray cluster launcher is part of the `ray` CLI. Use the CLI to start, stop and attach to a running ray cluster using commands such as `ray up`, `ray down` and `ray attach`. You can use pip to install the ray CLI with cluster launcher support. Follow [the Ray installation documentation](installation) for more detailed instructions.

```bash
# install ray
pip install -U ray[default]
```

## Install and Configure Google API Client

If you have never created a Google APIs Console project, read google Cloud's [Managing Projects page](https://cloud.google.com/resource-manager/docs/creating-managing-projects?visit_id=637952351450670909-433962807&rd=1) and create a project in the [Google API Console](https://console.developers.google.com/).
Next, install the Google API Client using `pip install -U google-api-python-client`.

```bash
# Install the Google API Client.
pip install google-api-python-client
```

## Start Ray with the Ray cluster launcher

Once the Google API client is configured to manage resources on your GCP account, you should be ready to launch your cluster. The provided [cluster config file](https://github.com/ray-project/ray/tree/master/python/ray/autoscaler/gcp/example-full.yaml) will create a small cluster with an on-demand n1-standard-2 head node and is configured to autoscale to up to two n1-standard-2 [preemptible workers](https://cloud.google.com/preemptible-vms/). Note that you'll need to fill in your GCP [project_id](https://github.com/ray-project/ray/blob/eacc763c84d47c9c5b86b26a32fd62c685be84e6/python/ray/autoscaler/gcp/example-full.yaml#L42) in those templates.

Test that it works by running the following commands from your local machine:

```bash
# Download the example-full.yaml
wget https://raw.githubusercontent.com/ray-project/ray/master/python/ray/autoscaler/gcp/example-full.yaml

# Edit the example-full.yaml to update project_id.
# vi example-full.yaml

# Create or update the cluster. When the command finishes, it will print
# out the command that can be used to SSH into the cluster head node.
ray up example-full.yaml

# Get a remote screen on the head node.
ray attach example-full.yaml

# Try running a Ray program.
python -c 'import ray; ray.init()'
exit

# Tear down the cluster.
ray down example-full.yaml
```

Congrats, you have started a Ray cluster on GCP!

## GCP Configurations

### Running workers with Service Accounts

By default, only the head node runs with a Service Account (`ray-autoscaler-sa-v1@<project-id>.iam.gserviceaccount.com`). To enable workers to run with this same Service Account (to access Google Cloud Storage, or GCR), add the following configuration to the worker_node configuration:

```yaml
available_node_types:
  ray.worker.default:
    node_config:
      ...
    serviceAccounts:
    - email: ray-autoscaler-sa-v1@<YOUR_PROJECT_ID>.iam.gserviceaccount.com
        scopes:
        - https://www.googleapis.com/auth/cloud-platform
```
