Examples
========

.. _virtual-cluster-deployment:

This page introduces examples about Ray virtual clusters:

.. contents::
    :local:

RayCluster Deployment
---------------------

This section demonstrates how to submit and deploy Ray Cluster to kubernetes using `kubectl <https://kubernetes.io/zh-cn/docs/reference/kubectl/>`_. The provided example showcases a RayCluster (`ray-cluster.sample.yaml <https://github.com/antgroup/ant-kuberay/blob/master/ray-operator/config/samples/ray-cluster.sample.yaml>`_) that interacts with kubernetes, retrieves RayCluster name, pods, services for cluster management.


**Create a ray cluster yaml file:** `ray-cluster.sample.yaml`

.. code-block:: yaml

   # This example config does not specify resource requests or limits.
   # For examples with more realistic resource configuration, see
   # ray-cluster.complete.large.yaml and
   # ray-cluster.autoscaler.large.yaml.
   apiVersion: ray.io/v1
   kind: RayCluster
   metadata:
     name: raycluster-sample
   spec:
     rayVersion: '2.9.0' # should match the Ray version in the image of the containers
     # Ray head pod template
     headGroupSpec:
       serviceType: ClusterIP # optional
       rayStartParams:
         dashboard-host: '0.0.0.0'
         block: 'true'
       #pod template
       template:
         metadata:
           labels:
             test: a
         spec:
           containers:
           - name: ray-head
             image: ray-image:antray-open
             resources:
               limits:
                 cpu: 1
                 memory: 2Gi
                 ephemeral-storage: 2Gi
               requests:
                 cpu: 500m
                 memory: 2Gi
                 ephemeral-storage: 2Gi
             env:
             - name: RAY_NODE_TYPE_NAME
               value: group1
             ports:
             - containerPort: 6379
               name: gcs-server
             - containerPort: 8265 # Ray dashboard
               name: dashboard
             - containerPort: 10001
               name: client
     workerGroupSpecs:
       # the pod replicas in this group typed worker
       - replicas: 1
         minReplicas: 1
         maxReplicas: 5
         # logical group name, for this called small-group, also can be functional
         groupName: small-group-0
         
         rayStartParams:
           block: 'true'
         #pod template
         template:
           metadata:
             labels:
               test: a
           spec:
             containers:
               - name: ray-worker # must consist of lower case alphanumeric characters or '-', and must start and end with an alphanumeric character (e.g. 'my-name',  or '123-abc'
                 image: ray-image:antray-open
                 env:
                 - name: RAY_NODE_TYPE_NAME
                   value: group0
                 resources:
                   limits:
                     cpu: 1
                     memory: 1Gi
                     ephemeral-storage: 1Gi
                   requests:
                     cpu: 500m
                     memory: 1Gi
                     ephemeral-storage: 1Gi
       # the pod replicas in this group typed worker
       - replicas: 1
         minReplicas: 1
         maxReplicas: 5
         # logical group name, for this called small-group, also can be functional
         groupName: small-group-1
         rayStartParams:
           block: 'true'
         #pod template
         template:
           metadata:
             labels:
               test: a
           spec:
             containers:
               - name: ray-worker # must consist of lower case alphanumeric characters or '-', and must start and end with an alphanumeric character (e.g. 'my-name',  or '123-abc'
                 image: ray-image:antray-open
                 env:
                 - name: RAY_NODE_TYPE_NAME
                   value: group1
                 resources:
                   limits:
                     cpu: 1
                     memory: 1Gi
                     ephemeral-storage: 1Gi
                   requests:
                     cpu: 500m
                     memory: 1Gi
                     ephemeral-storage: 1Gi

**Create RayCluster**

`kubectl create -f  ray-cluster.sample.yaml`

**Get RayCluster**

.. code-block:: text

   $ kubectl get rayclusters.ray.io --sort-by='{.metadata.creationTimestamp}'
   NAME                    DESIRED WORKERS   AVAILABLE WORKERS   STATUS   AGE
   raycluster-sample       1                                     ready    3d14h

**Get RayCluster Pods**

.. code-block:: text

   $ kubectl get po -owide --sort-by='{.metadata.creationTimestamp}'  -l ray.io/cluster=raycluster-sample
   NAME                                           READY   STATUS    RESTARTS   AGE     IP              NODE       NOMINATED NODE   READINESS GATES
   raycluster-sample-head-z779h                   1/1     Running   0          3d14h   100.88.92.34    yg61001t   <none>           1/1
   raycluster-sample-worker-small-group-0-lf9gg   1/1     Running   1          3d14h   100.88.92.192   yg61001t   <none>           1/1
   raycluster-sample-worker-small-group-1-lf9gg   1/1     Running   1          3d14h   100.88.92.191   yg61001t   <none>           1/1

.. _virtual-cluster-simple-job:

Simple Job
----------

This section demonstrates how to execute and submit Ray jobs to both Divisible and Indivisible Virtual Clusters using the Ray CLI. The provided example showcases a Python script (test.py) that interacts with different components within a Ray cluster, retrieves node IDs, and utilizes placement groups for resource management.

.. code-block:: python

    import ray
    import sys

    # Initialize Ray and connect to the existing cluster
    ray.init(address='auto')

    @ray.remote
    class Actor:
        def __init__(self):
            pass

        def run(self):
            # Retrieve and return the node ID where this actor is running
            return ray.get_runtime_context().get_node_id()

    @ray.remote
    def hello():
        # Retrieve and return the node ID where this task is running
        return ray.get_runtime_context().get_node_id()

    # Create a placement group with 1 CPU
    pg = ray.util.placement_group(
        bundles=[{"CPU": 1}], name="pg_name"
    )
    ray.get(pg.ready())

    # Execute a remote task to get the node ID
    node_id_task = ray.get(hello.remote())
    print("node_id:task: ", node_id_task)

    # Create a detached actor and get its node ID
    actor = Actor.options(name="test_actor", namespace="test", lifetime="detached").remote()
    node_id_actor = ray.get(actor.run.remote())
    print("node_id:actor: ", node_id_actor)

    # Get the node ID associated with the placement group
    placement_group_table = ray.util.placement_group_table(pg)
    node_id_pg = placement_group_table["bundles_to_node_id"][0]
    print("node_id:placement_group: ", node_id_pg)

Submitting to a Divisible Cluster
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Submitting a job to a Divisible Cluster involves specifying the --virtual-cluster-id and defining the replica sets.

**Command:**

`ray job submit --working-dir . --virtual-cluster-id kPrimaryClusterID --replica-sets '{"group0": 1}' -- python test.py`

**Command Breakdown:**

- --working-dir .: Sets the current directory as the working directory for the job.
- --virtual-cluster-id kPrimaryClusterID: Specifies the Divisible Cluster named PrimaryCluster to which the job is submitted.
- --replica-sets '{"group0": 1}': Defines the replica set configuration, requesting 1 replica in group0.
- -- python test.py: Indicates the Python script to execute.

Submitting to an Indivisible Cluster
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Submitting a job to a Indivisible Cluster involves specifying the --virtual-cluster-id

**Command:**

`ray job submit --working-dir . --virtual-cluster-id indivisibleLogicalID -- python test.py`

**Command Breakdown:**

- **--working-dir .:** Sets the current directory as the working directory for the job.
- **--virtual-cluster-id indivisibleLogicalID:** Specifies the Indivisible Cluster named indivisibleLogicalID to which the job is submitted.
- **--python test.py:** Indicates the Python script to execute.

.. _virtual-cluster-raydata-job:

RayData Job
-----------

Let's now submit a RayData job. The whole process is as same as submitting a simple job, where the execution parallelism will be deduced based on the certain virtual cluster's resources, and operator executor, i.e. Ray Actors, will also be restricted inside the virtual cluster.

A batch inference job
~~~~~~~~~~~~~~~~~~~~~

Since we only want to demonstrate the process, we simplify the job by mocking a inference model that returns True when the number of passengers is more than 2, and vice versa.

.. code-block:: python

    # Solution 1: Batch Inference with a Self-Maintained Pool of Actors
    print("Batch inference with a self-maintained pool of actors.")
    import pandas as pd
    import pyarrow.parquet as pq
    import ray

    def load_trained_model():
        # A fake model that predicts whether tips were given based on
        # the number of passengers in the taxi cab.
        def model(batch: pd.DataFrame) -> pd.DataFrame:
            # Give a tip if 2 or more passengers.
            predict = batch["passenger_count"] >= 2 
            return pd.DataFrame({"score": predict})
        return model

    model = load_trained_model()
    model_ref = ray.put(model)
    input_splits = [f"s3://anonymous@air-example-data/ursa-labs-taxi-data/downsampled_2009_full_year_data.parquet"
                    f"/fe41422b01c04169af2a65a83b753e0f_{i:06d}.parquet"
                    for i in range(12) ]

    ds = ray.data.read_parquet(input_splits)

    class CallableCls:
        def __init__(self, model):
            self.model = ray.get(model)

        def __call__(self, batch):
            result = self.model(batch)
            return result

    results = ds.map_batches(
        CallableCls,
        num_gpus=0,
        batch_size=1024,
        batch_format="numpy",
        compute=ray.data.ActorPoolStrategy(min_size=1, max_size=5),
        fn_constructor_args=(model_ref,))

    print(results.take(5))

Name it as NYC_taxi_predict.pyand put it to `/path/to/project/NYC_taxi_predict.py`.

Submitting to a Divisible Cluster
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Submitting the job is exactly as the same as before:

**Command:**

`ray job submit --working-dir /path/to/project/ --virtual-cluster-id kPrimaryClusterID --replica-sets '{"group0": 1}' -- python NYC_taxi_predict.py`

**Command Breakdown:**

- **--working-dir .:** Sets /path/to/project/ as the working directory for the job.
- **--virtual-cluster-id kPrimaryClusterID:** Specifies the Divisible Cluster named PrimaryCluster to which the job is submitted.
- **--replica-sets '{"group0": 1}':** Defines the replica set configuration, requesting 1 replica in group0.
- **--python NYC_taxi_predict.py:** Indicates the Python script to execute.

Submitting to an Indivisible Cluster
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Submitting a job to a Indivisible Cluster involves specifying the --virtual-cluster-id

**Command:**

`ray job submit --working-dir /path/to/project/ --virtual-cluster-id indivisibleLogicalID -- python NYC_taxi_predict.py`

**Command Breakdown:**

- **--working-dir .:** Sets the current directory as the working directory for the job.
- **--virtual-cluster-id indivisibleLogicalID:** Specifies the Indivisible Cluster named indivisibleLogicalID to which the job is submitted.
- **--python test.py:** Indicates the Python script to execute.
