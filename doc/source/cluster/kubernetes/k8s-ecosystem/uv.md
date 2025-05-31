(kuberay-uv)=
# Run RayCluster with uv.

This guide is aim to help for running a simple python script on RayCluster with `uv`. Starting from docker image `rayproject/ray:2.45.0`, `uv` is shipped within the image. It might benefit the development experience with using `uv` as package manager on RayCluster because of the speed of uv or the package of environment. 

## Prepare the yaml file

With the new feature in Ray 2.43, you could just kick off a python script by running `uv run ...`, dynamically creating runtime environment without the pre-built dependencies in the image.
```
RAY_RUNTIME_ENV_HOOK=ray._private.runtime_env.uv_runtime_env_hook.hook
```
For more information about this feature, please reference [here](https://www.anyscale.com/blog/uv-ray-pain-free-python-dependencies-in-clusters)

Here are some steps for preparing `ray-cluster.uv.yaml`:
- enable the new feature.
```yaml
env:
- name: RAY_RUNTIME_ENV_HOOK
  value: ray._private.runtime_env.uv_runtime_env_hook.hook
```
- prepare `sample_code.py` and mount on `/home/ray/samples`.
```python
import emoji
import ray

@ray.remote
def f():
  return emoji.emojize('Python is :thumbs_up:')

# Execute 10 copies of f across a cluster.
print(ray.get([f.remote() for _ in range(10)]))
```

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
