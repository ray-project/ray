(kuberay-modin-example)=

# Use Modin with Ray on Kubernetes

## Step 1: Install Kuberay operator

Follow the steps 1 and 2 from the [RayCluster Quick Start](kuberay-raycluster-quickstart) guide to install Kuberay operator.

## Step 2: Run the Modin example with RayJob

Create a file named `ray-job.modin.yaml` with the following content:

```yaml
apiVersion: ray.io/v1
kind: RayJob
metadata:
  name: rayjob-sample
spec:
  entrypoint: python /home/ray/samples/sample_code.py
  runtimeEnvYAML: |
    pip:
      - modin[all]==0.31.0
  rayClusterSpec:
    rayVersion: "2.31.0"
    headGroupSpec:
      rayStartParams:
        dashboard-host: "0.0.0.0"
      template:
        spec:
          containers:
            - name: ray-head
              image: rayproject/ray:2.31.0
              ports:
                - containerPort: 6379
                  name: gcs-server
                - containerPort: 8265
                  name: dashboard
                - containerPort: 10001
                  name: client
              resources:
                limits:
                  cpu: "1"
                requests:
                  cpu: "200m"
              volumeMounts:
                - mountPath: /home/ray/samples
                  name: code-sample
          volumes:
            - name: code-sample
              configMap:
                name: ray-job-code-sample
                items:
                  - key: sample_code.py
                    path: sample_code.py
    workerGroupSpecs:
      - replicas: 1
        minReplicas: 1
        maxReplicas: 5
        groupName: small-group
        rayStartParams: {}
        template:
          spec:
            containers:
              - name: ray-worker
                image: rayproject/ray:2.31.0
                lifecycle:
                  preStop:
                    exec:
                      command: ["/bin/sh", "-c", "ray stop"]
                resources:
                  limits:
                    cpu: "1"
                  requests:
                    cpu: "200m"
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: ray-job-code-sample
data:
  sample_code.py: |
    import ray
    import numpy as np
    import modin.pandas as pd
    import modin.config as modin_cfg

    ray.init()

    @ray.remote
    def modin_task():
        print("Modin Engine:", modin_cfg.Engine.get())
        return pd.DataFrame(
            {
                "A": 1.0,
                "B": pd.Timestamp("20130102"),
                "C": pd.Series(1, index=list(range(4)), dtype="float32"),
                "D": np.array([3] * 4, dtype="int32"),
                "E": pd.Categorical(["test", "train", "test", "train"]),
                "F": "foo",
            }
        )


    print(ray.get(modin_task.remote()))
```

And then run the following commands:

```sh
kubectl apply -f ray-job.modin.yaml
```

## Step 3: Check the output

Run the following command to check the output:

```sh
kubectl logs -l=job-name=rayjob-sample

# [Example output]
# 2024-06-30 06:27:01,584 INFO worker.py:1762 -- Connected to Ray # cluster. View the dashboard at 10.244.0.23:8265
# (modin_task pid=869) Modin Engine: Ray
#      A          B    C  D      E    F
# 0  1.0 2013-01-02  1.0  3   test  foo
# 1  1.0 2013-01-02  1.0  3  train  foo
# 2  1.0 2013-01-02  1.0  3   test  foo
# 3  1.0 2013-01-02  1.0  3  train  foo
# 2024-06-30 06:27:04,471 SUCC cli.py:60 -- # -----------------------------------
# 2024-06-30 06:27:04,471 SUCC cli.py:61 -- Job 'rayjob-sample-wfl9s' # succeeded
# 2024-06-30 06:27:04,471 SUCC cli.py:62 -- -----------------------------------
```
