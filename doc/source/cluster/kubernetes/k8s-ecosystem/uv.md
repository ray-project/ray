(kuberay-uv)=
# Run RayCluster with uv.

This guide is aim to help for running a simple python script on RayCluster with `uv`. Starting from docker image `rayproject/ray:2.45.0`, `uv` is shipped within the image. It might benefit the development experience with using `uv` as package manager on RayCluster because of the speed of uv or the package of environment. 

## Prepare the yaml file

With the new feature in Ray 2.43, you could just kick off a python script by running `uv run ...`, dynamically creating runtime environment without the pre-built dependencies in the image.
```
RAY_RUNTIME_ENV_HOOK=ray._private.runtime_env.uv_runtime_env_hook.hook
```
For more information about this feature, please reference [here](https://www.anyscale.com/blog/uv-ray-pain-free-python-dependencies-in-clusters)

<details>
  <summary>ray-cluster.uv.yaml</summary>

```yaml
apiVersion: ray.io/v1
kind: RayCluster
metadata:
  name: raycluster-uv
spec:
  rayVersion: '2.45.0' # should match the Ray version in the image of the containers
  headGroupSpec:
    rayStartParams: {}
    template:
      spec:
        containers:
        - name: ray-head
          image: rayproject/ray:2.45.0
          env:
           - name: RAY_RUNTIME_ENV_HOOK
             value: ray._private.runtime_env.uv_runtime_env_hook.hook
          resources:
            limits:
              cpu: 1
              memory: 2Gi
            requests:
              cpu: 500m
              memory: 2Gi
          ports:
          - containerPort: 6379
            name: gcs-server
          - containerPort: 8265 # Ray dashboard
            name: dashboard
          - containerPort: 10001
            name: client
          volumeMounts:
          - mountPath: /home/ray/samples
            name: code-sample
        volumes:
          - name: code-sample
            configMap:
              name: ray-uv-code-sample
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
          image: rayproject/ray:2.45.0
          env:
           - name: RAY_RUNTIME_ENV_HOOK
             value: ray._private.runtime_env.uv_runtime_env_hook.hook
          resources:
            limits:
              cpu: "1"
              memory: "1G"
            requests:
              cpu: "500m"
              memory: "1G"
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: ray-uv-code-sample
data:
  sample_code.py: |
    import emoji
    import ray
  
    @ray.remote
    def f():
        return emoji.emojize('Python is :thumbs_up:')
        # Execute 10 copies of f across a cluster.
  
    print(ray.get([f.remote() for _ in range(10)]))
```
</details>

## Run with uv

Apply the RayCluster.
```bash
kubectl apply -f ray-cluster.uv.yaml
```

Run `sample_code.py` via `uv`:
```bash
export HEAD_POD=$(kubectl get pods --selector=ray.io/node-type=head -o custom-columns=POD:metadata.name --no-headers)
kubectl exec -it $HEAD_POD -- /bin/bash -c "cd samples && uv run --with emoji /home/ray/samples/sample_code.py"
```
NOTE: use `/bin/bash -c` to execute for changing the current directory to `/home/ray/samples`, the default `working_dir` would be the current directory. This could avoid the uploading the files under `/home/ray` when executing `uv run`. Or, you could use `ray job submit --runtime-env-json ...` to specify the `working_dir` manually.
