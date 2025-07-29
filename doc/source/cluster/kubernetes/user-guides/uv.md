(kuberay-uv)=

# Using `uv` for Python package management in KubeRay

[uv](https://github.com/astral-sh/uv) is a modern Python package manager written in Rust.

Starting with Ray 2.45, the `rayproject/ray:2.45.0` image includes `uv` as one of its dependencies.
This guide provides a simple example of using `uv` to manage Python dependencies on KubeRay.

To learn more about the `uv` integration in Ray, refer to:

* [Environment Dependencies](https://docs.ray.io/en/latest/ray-core/handling-dependencies.html#using-uv-for-package-management)
* [uv + Ray: Pain-Free Python Dependencies in Clusters](https://www.anyscale.com/blog/uv-ray-pain-free-python-dependencies-in-clusters)

# Example

## Step 1: Create a Kind cluster

```sh
kind create cluster
```

## Step 2: Install KubeRay operator

Follow the [KubeRay Operator Installation](kuberay-operator-deploy) to install the latest stable KubeRay operator by Helm repository.

## Step 3: Create a RayCluster with `uv` enabled

`ray-cluster.uv.yaml` YAML file contains a RayCluster custom resource and a ConfigMap that includes a sample Ray Python script.
* The `RAY_RUNTIME_ENV_HOOK` feature flag enables the `uv` integration in Ray. Future versions may enable this by default.
    ```yaml
    env:
    - name: RAY_RUNTIME_ENV_HOOK
      value: ray._private.runtime_env.uv_runtime_env_hook.hook
    ```
* `sample_code.py` is a simple Ray Python script that uses the `emoji` package.
    ```python
    import emoji
    import ray
    
    @ray.remote
    def f():
        return emoji.emojize('Python is :thumbs_up:')
    
    # Execute 10 copies of f across a cluster.
    print(ray.get([f.remote() for _ in range(10)]))
    ```

```sh
kubectl apply -f https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/ray-cluster.uv.yaml
```

## Step 4: Execute a Ray Python script with `uv`

```sh
export HEAD_POD=$(kubectl get pods --selector=ray.io/node-type=head -o custom-columns=POD:metadata.name --no-headers)
kubectl exec -it $HEAD_POD -- /bin/bash -c "cd samples && uv run --with emoji /home/ray/samples/sample_code.py"

# [Example output]:
#
# Installed 1 package in 1ms
# 2025-06-01 14:49:15,021 INFO worker.py:1554 -- Using address 127.0.0.1:6379 set in the environment variable RAY_ADDRESS
# 2025-06-01 14:49:15,024 INFO worker.py:1694 -- Connecting to existing Ray cluster at address: 10.244.0.6:6379...
# 2025-06-01 14:49:15,035 INFO worker.py:1879 -- Connected to Ray cluster. View the dashboard at 10.244.0.6:8265
# 2025-06-01 14:49:15,040 INFO packaging.py:576 -- Creating a file package for local module '/home/ray/samples'.
# 2025-06-01 14:49:15,041 INFO packaging.py:368 -- Pushing file package 'gcs://_ray_pkg_d4da2ce33cf6d176.zip' (0.00MiB) to Ray cluster...
# 2025-06-01 14:49:15,042 INFO packaging.py:381 -- Successfully pushed file package 'gcs://_ray_pkg_d4da2ce33cf6d176.zip'.
# ['Python is 👍', 'Python is 👍', 'Python is 👍', 'Python is 👍', 'Python is 👍', 'Python is 👍', 'Python is 👍', 'Python is 👍', 'Python is 👍', 'Python is 👍']
```

> NOTE: Use `/bin/bash -c` to execute the command while changing the current directory to `/home/ray/samples`. By default, `working_dir` is set to the current directory.
This prevents uploading all files under `/home/ray`, which can take a long time when executing `uv run`.
Alternatively, you can use `ray job submit --runtime-env-json ...` to specify the `working_dir` manually.
