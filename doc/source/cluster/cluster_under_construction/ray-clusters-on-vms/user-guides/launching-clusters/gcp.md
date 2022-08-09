
# Launching Ray Clusters on GCP

This guide details the steps needed to start a Ray cluster in GCP.

To start a GCP Ray cluster, you need to use the Ray Cluster Launcher with Google API client.


## Install Ray Cluster Launcher
The Ray Cluster Launcher is part of the `ray` command line tool. It allows you to start, stop and attach to a running ray cluster using commands such as  `ray up`, `ray down` and `ray attach`. You can use pip to install the ray command line tool with cluster launcher support. Follow [install ray](https://docs.ray.io/en/latest/ray-overview/installation.html) for more detailed instructions.

```
# install ray
pip install -U ray[default]
```

## Install and Configure Google API Client

If you have never created a Google APIs Console project, read the [Managing Projects page](https://cloud.google.com/resource-manager/docs/creating-managing-projects?visit_id=637952351450670909-433962807&rd=1) and create a project in the [Google API Console](https://console.developers.google.com/).
Next, install Google API Client using `pip install -U google-api-python-client`.


```
# install Google API Client
pip install google-api-python-client
```

## Start Ray with the Ray Cluster Launcher

Once the API client is configured to manage resources on your GCP account, you should be ready to launch your cluster. The provided [example-full.yaml](https://github.com/ray-project/ray/tree/master/python/ray/autoscaler/gcp/example-full.yaml) cluster config file will create a small cluster with a n1-standard-2 head node (on-demand) configured to autoscale up to two n1-standard-2 [preemptible workers](https://cloud.google.com/preemptible-vms/). Note that you'll need to fill in your [project_id](https://github.com/ray-project/ray/blob/eacc763c84d47c9c5b86b26a32fd62c685be84e6/python/ray/autoscaler/gcp/example-full.yaml#L42) in those templates.


Test that it works by running the following commands from your local machine:

```
# Download the example-full.yaml
wget https://github.com/ray-project/ray/blob/master/python/ray/autoscaler/gcp/example-full.yaml

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

