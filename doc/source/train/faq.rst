.. _train-faq:

Ray Train FAQ
=============

How fast is Ray Train compared to PyTorch, TensorFlow, etc.?
------------------------------------------------------------

At its core, training speed should be the same - while Ray Train launches distributed training workers via Ray Actors,
communication during training (e.g. gradient synchronization) is handled by the backend training framework itself.

For example, when running Ray Train with the ``TorchTrainer``,
distributed training communication is done with Torch's ``DistributedDataParallel``.

Take a look at the :ref:`Pytorch <pytorch-training-parity>` and :ref:`Tensorflow <tf-training-parity>` benchmarks to check performance parity.

How do I set resources?
-----------------------

By default, each worker will reserve 1 CPU resource, and an additional 1 GPU resource if ``use_gpu=True``.

To override these resource requests or request additional custom resources,
you can initialize the ``Trainer`` with ``resources_per_worker`` specified in ``scaling_config``.

.. note::
   Some GPU utility functions (e.g. :func:`ray.train.torch.get_device`, :func:`ray.train.torch.prepare_model`)
   currently assume each worker is allocated exactly 1 GPU. The partial GPU and multi GPU use-cases
   can still be run with Ray Train today without these functions.


My multi-node  PyTorch GPU training is hanging or giving me obscure NCCL errors. What do I do?
----------------------------------------------------------------------------------------------
If you are on a multi-node GPU training setup and training is hanging, or you get errors like
`RuntimeError: NCCL error in: /pytorch/torch/lib/c10d/ProcessGroupNCCL.cpp:911, unhandled system error`
it could be that there is some networking misconfiguration in your cluster.

To resolve these issues, you can do the following:

1. First run the `ifconfig` command to get the supported network interfaces for your machine. You can install `ifconfig` via `sudo apt install net-tools`.
   You should get an output like so:

    .. code::

        docker0: flags=4163<UP,BROADCAST,RUNNING,MULTICAST>  mtu 1500
                inet 172.17.0.1  netmask 255.255.0.0  broadcast 172.17.255.255
                inet6 fe80::42:4cff:fe7e:eda  prefixlen 64  scopeid 0x20<link>
                ether 02:42:4c:7e:0e:da  txqueuelen 0  (Ethernet)
                RX packets 24041  bytes 94360851 (94.3 MB)
                RX errors 0  dropped 0  overruns 0  frame 0
                TX packets 24044  bytes 2216396 (2.2 MB)
                TX errors 0  dropped 0 overruns 0  carrier 0  collisions 0

        ens5: flags=4163<UP,BROADCAST,RUNNING,MULTICAST>  mtu 9001
                inet 172.31.65.244  netmask 255.255.224.0  broadcast 172.31.95.255
                inet6 fe80::81c:ddff:fe05:a5f1  prefixlen 64  scopeid 0x20<link>
                ether 0a:1c:dd:05:a5:f1  txqueuelen 1000  (Ethernet)
                RX packets 1237256  bytes 911474939 (911.4 MB)
                RX errors 0  dropped 0  overruns 0  frame 0
                TX packets 1772254  bytes 2265089819 (2.2 GB)
                TX errors 0  dropped 0 overruns 0  carrier 0  collisions 0

        lo: flags=73<UP,LOOPBACK,RUNNING>  mtu 65536
                inet 127.0.0.1  netmask 255.0.0.0
                inet6 ::1  prefixlen 128  scopeid 0x10<host>
                loop  txqueuelen 1000  (Local Loopback)
                RX packets 2734593  bytes 6775739628 (6.7 GB)
                RX errors 0  dropped 0  overruns 0  frame 0
                TX packets 2734593  bytes 6775739628 (6.7 GB)
                TX errors 0  dropped 0 overruns 0  carrier 0  collisions 0

        veth526c8fe: flags=4163<UP,BROADCAST,RUNNING,MULTICAST>  mtu 1500
                inet6 fe80::44c:7bff:fe80:f02b  prefixlen 64  scopeid 0x20<link>
                ether 06:4c:7b:80:f0:2b  txqueuelen 0  (Ethernet)
                RX packets 24041  bytes 94697425 (94.6 MB)
                RX errors 0  dropped 0  overruns 0  frame 0
                TX packets 24062  bytes 2217752 (2.2 MB)
                TX errors 0  dropped 0 overruns 0  carrier 0  collisions 0
2. Choose the network interface that corresponds to the private IP address of your node. In most cases, this will be either
   `ens3` or `ens5`.

3. Set this as the value for the `NCCL_SOCKET_IFNAME` environment variable. You must do this via Ray runtime environments so that it
   gets propagated to all training workers.

.. code-block:: python

    # Add this at the top of your Ray application.
    runtime_env = {"env_vars": {"NCCL_SOCKET_IFNAME": "ens5"}}
    ray.init(runtime_env=runtime_env)



