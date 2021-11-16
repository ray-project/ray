Best Practices: Ray with Jupyter Notebook / JupyterLab
======================================================

This document describes best practices for using Ray with Jupyter Notebook / 
JupyterLab. We use AWS for the purpose of illustrion, but the arguments 
should also apply to other Cloud providers.
Feel free to contribute if you think this document is 
missing anything.

Setting Up Notebook
-------------------

1. Ensure your EC2 instance has enough EBS volume if you plan to run the 
Notebook on it.
The Deep Learning AMI, pre-installed libraries and environmental set-up 
will by default consume ~76% of the disk prior to any Ray work.
With additional applications running, the Notebook could fail frequently
due to full disk. 
Kernel restart loses progressing cell outputs, especially if we rely on 
them to track experiment progress. 
Related issue: `Autoscaler should allow configuration of disk space and 
should use a larger default. <https://github.com/ray-project/ray/issues/1376>`_.

2. Avoid unnecessary memory usage.
IPython stores the output of every cell in a local Python variable
indefinitely. This causes Ray to pin the objects even though you application
may not actually be using them.
Therefore, explicitly calling ``print`` or ``repr`` is better than letting 
the Notebook automatically generate the output.
Another option is to just altogether disable IPython caching with the 
following (run from bash/zsh):

.. code-block:: console

    echo 'c = get_config()
    c.InteractiveShell.cache_size = 0 # disable cache
    ' >>  ~/.ipython/profile_default/ipython_config.py

This will still allow printing, but stop IPython from caching altogether.

.. tip::
  While the above settings help reduce memory footprint, it's always a good 
  practice to remove references that are no longer needed in your application
  to free space in the object store.

3. Understand the node’s responsibility. 
Assuming the Notebook runs on a EC2 instance,
do you plan to start a ray runtime locally on this instance,
or do you plan to use this instance as a cluster launcher? 
Jupyter Notebook is more suitable for the first scenario. 
CLI’s such as ``ray exec`` and ``ray submit`` fit the second use case better.

4. Forward the ports.
Assuming the Notebook runs on an EC2 instance,
you should forward both the Notebook port and the Ray Dashboard port.
The default ports are 8888 and 8265 respectively. 
They will increase if the default ones are not available.
You can forward them with the following (run from bash/zsh):

.. code-block:: console

    ssh -i /path/my-key-pair.pem -N -f -L localhost:8888:localhost:8888 my-instance-user-name@my-instance-IPv6-address
    ssh -i /path/my-key-pair.pem -N -f -L localhost:8265:localhost:8265 my-instance-user-name@my-instance-IPv6-address
