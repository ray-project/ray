# KubeRay

```{admonition} What is Kuberay?
[KubeRay](https://github.com/ray-project/kuberay) is a set of tools for running Ray on Kubernetes.
It has been used by some larger corporations to deploy Ray on their infrastructure.
Going forward, we would like to make this way of deployment accessible and seamless for
all Ray users and standardize Ray deployment on Kubernetes around KubeRay's operator.
Presently you should consider this integration a minimal viable product that is not polished
enough for general use and prefer the [Kubernetes integration](kubernetes.rst) for running
Ray on Kubernetes. If you are brave enough to try the KubeRay integration out, this documentation
is for you! We would love your feedback as a [Github issue](https://github.com/ray-project/ray/issues)
including `[KubeRay]` in the title.
```


## Deployment on Kubernetes

Most organizations have now adopted Kubernetes as their cluster resource
management solution. You can use KubeRay to deploy Ray on a Kubernetes
cluster.

The KubeRay project is the community standard way of deployment on
Kubernetes and offers an Kubernetes operator for managing Ray clusters.

The KubeRay operator (Ray Operator) helps deploy and manage Ray clusters
on top of Kubernetes. Clusters are defined as a custom
\`\`RayCluster\`\` resource and managed by a fault-tolerant Ray
controller. The operator automates provisioning, management, autoscaling
and operations of Ray clusters deployed to Kubernetes.

Some of the main features of the operator are:

-   Management of first-class RayClusters via a custom resource.

-   Support for heterogenous worker types in a single Ray cluster.

-   Built-in monitoring via Prometheus.

-   Use of PodTemplate to create Ray pods

-   Updated status based on the running pods

-   Automatically populate environment variables in the containers

-   Automatically prefix your container command with the ray start
    command

-   Automatically adding the volumeMount at /dev/shm for shared memory

-   Use of ScaleStrategy to remove specific nodes in specific groups

### Starting your first Kuberay Cluster

The following instructions are for Minikube but the deployment works the same way on a real Kubernetes cluster. You need to have at
least 4 CPUs to run this example. 

```shell
# First we make sure Minikube is initialized.
# Only do this if you want to try out KubeRay locally:
minikube start
```

You can deploy the operator by checking out the Ray repository
(<https://github.com/ray-project/ray.git>) and calling:


```bash
ray/python/ray/autoscaler/kuberay/init-config.sh
kubectl apply -k "ray/python/ray/autoscaler/kuberay/config/default"
kubectl apply -f "ray/python/ray/autoscaler/kuberay/kuberay-autoscaler-rbac.yaml"
```
You can verify that the operator has been deployed using

```bash
kubectl -n ray-system get pods
```

When deployed, the operator will watch for K8s events
(create/delete/update) for the \`\`raycluster\`\` resource updates. Upon
these events, the operator can create a cluster consisting of a head pod
and multiple worker pods, delete a cluster, or update the cluster by
adding or removing worker pods.

Now let’s deploy a new Ray cluster using a provided default cluster
configuration. We’ll cover how to configure this YAML file in a later
section:

```bash
kubectl create -f ray/python/ray/autoscaler/kuberay/ray-cluster.complete.yaml
```

The KubeRay operator configures a Kubernetes service targeting the Ray
head pod.


> **_NOTE:_**  The example config ray-cluster.complete.yaml specifies rayproject/ray:8c5fe4
> as the Ray autoscaler image. This image carries the latest improvements to KubeRay autoscaling
> support. This autoscaler image is confirmed to be compatible with Ray versions >= 1.11.0.
> Once Ray autoscaler support is stable, the recommended pattern will be to use the same
> Ray version in the autoscaler and Ray containers.


To identify the service, run
`kubectl get service --selector=ray.io/cluster=raycluster-complete`. The
output should resemble \`\`\` NAME TYPE CLUSTER-IP EXTERNAL-IP PORT(S)
AGE raycluster-complete-head-svc ClusterIP xx.xx.xxx.xx &lt;none&gt;
6379/TCP,8265/TCP,10001/TCP 6m10s \`\`\` The three ports indicated in
the output correspond to the following services of the Ray head pod.

-   6379: The Ray head’s GCS service. Ray worker pods connect to this
    service when joining the cluster.

-   8265: Exposes the Ray Dashboard and the Ray Job Submission service.

-   10001: Exposes the Ray Client server.

### Interacting with the KubeRay cluster

Now, you might be wondering, "This isn’t useful — I am interested in
figuring out how to run my Ray script on Kubernetes, not learning about
Kubernetes".

That’s understandable, and we’ll get to that in a moment. First, let’s
use this below python script as the desired script to run on the
cluster. We’ll name it \`\`script.py\`\` (for simplicity), and the
script will connect to the Ray cluster and run a couple standard Ray
commands:

    # tag::cluster_connect[]
    # script.py

    import ray
    ray.init(address="auto")
    print(ray.cluster_resources())

    @ray.remote
    def test():
        return 12

    ray.get([test.remote() for i in range(12)])
    # end::cluster_connect[]

    # tag::ray_client[]
    # script.py

    import ray
    ray.init(address="ray://localhost:10001")
    print(ray.cluster_resources())

    @ray.remote
    def test():
        return 12

    ray.get([test.remote() for i in range(12)])
    # end::ray_client[]

To run this script, there are three primary ways I will cover:

-   kubectl exec

-   Ray Job Submission

-   Ray Client

#### Running Ray programs with \`\`kubectl\`\`

You can directly interact with the head pod via \`\`kubectl exec\`\`.
Use the command below to get a Python interpreter on the head pod:

    kubectl exec `kubectl get pods -o custom-columns=POD:metadata.name | grep raycluster-complete-head` -it -c ray-head -- python

With this python terminal, you can connect and run your own Ray
application:

    import ray
    ray.init(address="auto")
    ...

There are other ways of interacting with these services without
\`\`kubectl\`\`, but these will require some networking setup.

The easiest route and the one we’ll take for the examples below is to
use port-forwarding.

#### Using the Ray Job Submission Server

You can run scripts on the cluster by using the Ray Job Submission
server. You can use the server to send a script or a bundle of
dependencies, and run custom scripts with that set of dependencies.

To start, you’ll need to port-forward the job submission server port:

    kubectl port-forward service/raycluster-complete-head-svc 8265:8265

Now, submit the script by setting the \`\`RAY\_ADDRESS\`\` variable to
the job server submission endpoint and using the Ray Job Submission CLI.

    export RAY_ADDRESS="localhost:8265"
    ray job submit --working-dir=. -- python script.py

You’ll see an output that looks like:

    # tag::start_head[]
    $ ray start --head --port=6379
    ...
    Next steps
    To connect to this Ray runtime from another node, run
      ray start --address='<ip address>:6379'

    If connection fails, check your firewall settings and network configuration.
    # end::start_head[]


    # tag::start_worker[]
    $ ray start --address=<address>
    --------------------
    Ray runtime started.
    --------------------

    To terminate the Ray runtime, run
      ray stop
    # end::start_worker[]

    # tag::job_submission[]
    Job submission server address: http://127.0.0.1:8265
    2022-05-20 23:35:36,066 INFO dashboard_sdk.py:276 -- Uploading package gcs://_ray_pkg_533a957683abeba8.zip.
    2022-05-20 23:35:36,067 INFO packaging.py:416 -- Creating a file package for local directory '.'.

    -------------------------------------------------------
    Job 'raysubmit_U5hfr1rqJZWwJmLP' submitted successfully
    -------------------------------------------------------

    Next steps
      Query the logs of the job:
        ray job logs raysubmit_U5hfr1rqJZWwJmLP
      Query the status of the job:
        ray job status raysubmit_U5hfr1rqJZWwJmLP
      Request the job to be stopped:
        ray job stop raysubmit_U5hfr1rqJZWwJmLP

    Tailing logs until the job exits (disable with --no-wait):
    {'memory': 47157884109.0, 'object_store_memory': 2147483648.0, 'CPU': 16.0, 'node:127.0.0.1': 1.0}

    ------------------------------------------
    Job 'raysubmit_U5hfr1rqJZWwJmLP' succeeded
    ------------------------------------------
    # end::job_submission[]

You can use \`\`--no-wait\`\` to run the job in the background.

#### Ray Client

To connect to the cluster via Ray Client from your local machine, first
make sure the local Ray installation and Python minor version match the
Ray and Python versions running in the Ray cluster.

Now run the following command.
`kubectl port-forward service/raycluster-complete-head-svc 10001:10001`.
This command will block. The local port 10001 will now be forwarded to
the Ray head’s Ray Client server.

To run a Ray workload on your remote Ray cluster, open a local Python
shell and start a Ray Client connection:

    # tag::cluster_connect[]
    # script.py

    import ray
    ray.init(address="auto")
    print(ray.cluster_resources())

    @ray.remote
    def test():
        return 12

    ray.get([test.remote() for i in range(12)])
    # end::cluster_connect[]

    # tag::ray_client[]
    # script.py

    import ray
    ray.init(address="ray://localhost:10001")
    print(ray.cluster_resources())

    @ray.remote
    def test():
        return 12

    ray.get([test.remote() for i in range(12)])
    # end::ray_client[]

With this method, you can just run the Ray program directly on your
laptop (instead of needing to ship the code over via kubectl or job
submission).

### Exposing KubeRay

In the above examples, we used port-forwarding as a simple way access
the Ray head’s services. For production use cases, you may want to
consider other means of exposing these services. The following notes are
generic to services running on Kubernetes. Refer to the Kubernetes
documentation for more detailed info.

By default, the Ray service is accessible from anywhere **within** the
Kubernetes cluster where the Ray Operator / cluster is running. For
example to use the Ray Client from a pod in the same Kubernetes
namespace as the Ray Cluster, use
`ray.init("ray://raycluster-complete-head-svc:10001")`. To connect from
another Kubernetes namespace, use
`ray.init("ray://raycluster-complete-head-svc.default.svc.cluster.local:10001")`.
(If the Ray cluster is a non-default namespace, use the namespace in
place of `default`.)

If you are trying to access the service from outside the cluster, use an
ingress controller. Any standard ingress controller should work with Ray
Client and Ray Dashboard. Pick a solution compatible your networking and
security requirements — further guidance is out of scope for this book.

### Configuring KubeRay

Let’s take a closer look at the configuration for a Ray cluster running
on K8s. The example file
`ray/python/ray/autoscaler/kuberay/ray-cluster.complete.yaml` is a good
reference. Here is a condensed view of a RayCluster config’s most
salient features:

    apiVersion: ray.io/v1alpha1
    kind: RayCluster
    metadata:
      name: raycluster-complete
    spec:
      headGroupSpec:
        rayStartParams:
          port: '6379'
          num-cpus: '1'
          ...
        template: # Pod template
            metadata: # Pod metadata
            spec: # Pod spec
                containers:
                - name: ray-head
                  image: rayproject/ray:1.12.1
                  resources:
                    limits:
                      cpu: "1"
                      memory: "1024Mi"
                    requests:
                      cpu: "1"
                      memory: "1024Mi"
                  ports:
                  - containerPort: 6379
                    name: gcs
                  - containerPort: 8265
                    name: dashboard
                  - containerPort: 10001
                    name: client
                  env:
                    - name: "RAY_LOG_TO_STDERR"
                      value: "1"
                  volumeMounts:
                    - mountPath: /tmp/ray
                      name: ray-logs
                volumes:
                - name: ray-logs
                  emptyDir: {}
      workerGroupSpecs:
      - groupName: small-group
        replicas: 2
        rayStartParams:
            ...
        template: # Pod template
            ...
      - groupName: medium-group
        ...

Below, we cover some of the primary configuration values that you may
use.

**headGroupSpec and workerGroupSpecs**: A Ray cluster consists of a head
pod and a number of worker pods. The head pod’s configuration is
specified under `headGroupSpec`. Configuration for worker pods is
specified under `workerGroupSpecs`. There may be multiple worker groups,
each group with it’s own configuration `template`. The `replicas` field
of a `workerGroup` specifies the number of worker pods of each group to
keep in the cluster.

**rayStartParams**: This is a string-string map of arguments to the Ray
pod’s `ray start` entrypoint. For the full list of arguments, refer to
the documentation for `ray start`. We make special note of the
`num-cpus` and `num-gpus` field arguments:

-   **num-cpus**: This field tells the Ray scheduler how many CPUs are
    available to the Ray pod. The CPU count can be autodetected from the
    Kubernetes resource limits specified in the group spec’s pod
    `template`. It is sometimes useful to override this autodetected
    value. For example, setting `num-cpus:"0"` will prevent Ray
    workloads with non-zero CPU requirements from being scheduled on the
    head node.

-   **num-gpus**: This specifies the number of GPUs available to the Ray
    pod. At the time of writing, this field is **not** detected from the
    group spec’s pod `template`. Thus, `num-gpus` must be set explicitly
    for GPU workloads.

**template** This is where the bulk of the `headGroup` or
`workerGroup`'s configuration goes. The `template` is a Kubernetes Pod
template which determines the configuration for the pods in the group.
Here are some of the fields to pay attention to: - **resources**; It’s
important to specify container CPU and memory requests and limits for
each group spec. For GPU workloads, you may also wish to specify GPU
limits e.g. `nvidia.com/gpu: 1` if using an nvidia GPU device plugin.

It is ideal when possible to size each Ray pod such that it takes up the
entire Kubernetes node on which it is scheduled. In other words, it’s
best to run one large Ray pod per Kubernetes node; running multiple Ray
pods on one Kubernetes node introduces unecessary overhead. However,
there are situations in which running multiple Ray pods on one K8s node
makes sense: - Many users are running Ray clusters on a Kubernetes
cluster with limited compute resources. - You or your organization are
not directly managing Kubernetes nodes (e.g. when deploying on GCP’s GKE
Autopilot).

-   **nodeSelector and tolerations**: You can control the scheduling of
    worker group’s Ray pods by setting the `nodeSelector` and
    `tolerations` fields of the pod spec. Specifically, these fields
    determine on which Kubernetes nodes the pods may be scheduled. Note
    that the KubeRay operator operates at the level of pods — KubeRay is
    agnostic to the setup of the underlying Kubernetes nodes. Kubernetes
    node configuration is left to your Kubernetes cluster’s admins.

-   **Ray container images**: It’s important to specify the images used
    by your cluster’s Ray containers. The head and workers of the
    clusters should all use the same Ray version. In most cases, it
    makes sense to the exact same container image for the head and all
    workers of a given Ray cluster. To specify custom dependencies for
    your cluster, it’s recommended to build an image based on one of the
    official `rayproject/ray` images.

-   **Volume mounts**: Volume mounts can be used to preserve logs or
    other application data originating in your Ray containers. (See the
    logging section below.)

-   **Container environment variables**: Container environment variables
    may be used to modify Ray’s behavior. For example,
    `RAY_LOG_TO_STDERR` will redirect logs to STDERR rather than writing
    them to container’s file system. (See logging section below.)

### Configuring Logging for KubeRay

Ray cluster processes typically write logs to the directory
`/tmp/ray/session_latest/logs` in the pod. These logs are also viewable
in the Ray Dashboard.

To persist Ray logs beyond the lifetime of a pod, you may use one of the
following techniques.

**Aggregate logs from the container’s file system.**: For this strategy,
mount an empty-dir volume with mountPath `/tmp/ray/` in the Ray
container. (See the example configuration above.) You can mount the log
volume into a sidecar container running a log aggregation tool such as
Promtail.

**Container STDERR logging**: An alternative is to redirect logging to
STDERR. To do this set the environment variable `RAY_LOG_TO_STDERR=1` on
all Ray containers. In terms of K8s configuration, that means adding an
entry to the `env` field of the Ray container in each Ray `groupSpec`.

    env:
        ...
        - name: "RAY_LOG_TO_STDERR"
          value: "1"
        ...

You may then using a Kubernetes logging tool geared towards aggregation
from the STDERR and STDOUT streams.

### Uninstalling the KubeRay operator

You can uninstall the KubeRay operator using
```shell
kubectl delete -f "ray/python/ray/autoscaler/kuberay/kuberay-autoscaler-rbac.yaml"
kubectl delete -k "ray/python/ray/autoscaler/kuberay/config/default"
```

Note that all running Ray clusters will automatically be terminated.


## Developing the KubeRay integration (advanced)

### Developing the KubeRay operator
If you also want to change the underlying KubeRay operator, please refer to the instructions
in [the KubeRay development documentation](https://github.com/ray-project/kuberay/blob/master/ray-operator/DEVELOPMENT.md). In that case you should push the modified operator to your docker account or registry and
follow the instructions in `ray/python/ray/autoscaler/kuberay/init-config.sh`.

### Developing the Ray autoscaler code
Code for the Ray autoscaler's KubeRay integration is located in `ray/python/ray/autoscaler/_private/kuberay`.

Here is one procedure to test development autoscaler code.
1. Push autoscaler code changes to your fork of Ray.
2. Use the following Dockerfile to build an image with your changes.
```dockerfile
# Use the latest Ray master as base.
FROM rayproject/ray:nightly
# Invalidate the cache so that fresh code is pulled in the next step.
ARG BUILD_DATE
# Retrieve your development code.
RUN git clone -b <my-dev-branch> https://github.com/<my-git-handle>/ray
# Install symlinks to your modified Python code.
RUN python ray/python/ray/setup-dev.py -y
```
3. Push the image to your docker account or registry. Assuming your Dockerfile is named "Dockerfile":
```shell
docker build --build-arg BUILD_DATE=$(date +%Y-%m-%d:%H:%M:%S) -t <registry>/<repo>:<tag> - < Dockerfile
docker push <registry>/<repo>:<tag>
```
4. Update the autoscaler image in `ray-cluster.complete.yaml`

Refer to the [Ray development documentation](https://docs.ray.io/en/latest/development.html#building-ray-python-only) for
further details.
