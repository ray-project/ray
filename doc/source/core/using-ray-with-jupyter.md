---
myst:
  html_meta:
    description: "Run Ray inside Jupyter Notebook and JupyterLab, covering notebook setup and connecting to an existing cluster."
---

# Use Ray with Jupyter Notebook and JupyterLab

This page describes best practices for using Ray with Jupyter Notebook and JupyterLab. The examples use AWS, but the advice should also apply to other cloud providers. If you think this page is missing anything, contribute an update.

(setting-up-notebook)=

## Set up the notebook

1\. Provision enough disk space. If you plan to run the notebook on an EC2 instance, make sure the instance has enough Amazon Elastic Block Store (EBS) volume space. By default, the Deep Learning AMI, preinstalled libraries, and environment setup consume about 76% of the disk before any Ray work. With other applications running, the notebook might fail frequently because the disk is full. A kernel restart loses the outputs of running cells, which matters most when you rely on those outputs to track experiment progress. For background, see the related issue [Autoscaler should allow configuration of disk space and should use a larger default](https://github.com/ray-project/ray/issues/1376).

2\. Avoid unnecessary memory usage. IPython stores the output of every cell in a local Python variable indefinitely, which causes Ray to pin the objects even when your application might not use them. Call `print` or `repr` explicitly instead of letting the notebook generate the output automatically. You can also disable IPython caching altogether. Run the following command in `bash` or `zsh`:

```console
echo 'c = get_config()
c.InteractiveShell.cache_size = 0 # disable cache
' >>  ~/.ipython/profile_default/ipython_config.py
```

Printing still works, but IPython stops caching output altogether.

:::{tip}
The preceding settings help reduce memory usage. To free space in the object store, also remove references that your application no longer needs.
:::

3\. Decide the node's role. If the notebook runs on an EC2 instance, decide whether you plan to start a Ray runtime locally on the instance or use the instance as a cluster launcher. Jupyter Notebook suits the first case better. CLI commands such as `ray exec` and `ray submit` suit the second case better.

4\. Forward the ports. If the notebook runs on an EC2 instance, forward both the notebook port and the Ray dashboard port. The default ports are 8888 and 8265, respectively. If a default port isn't available, the port number increases. To forward the ports, run the following commands in `bash` or `zsh`:

```console
ssh -i /path/my-key-pair.pem -N -f -L localhost:8888:localhost:8888 my-instance-user-name@my-instance-IPv6-address
ssh -i /path/my-key-pair.pem -N -f -L localhost:8265:localhost:8265 my-instance-user-name@my-instance-IPv6-address
```
