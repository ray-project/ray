RaySGD: Distributed Deep Learning
=================================

.. image:: raysgdlogo.png
    :scale: 20%
    :align: center

RaySGD is a lightweight library for distributed deep learning, providing thin wrappers around framework-native modules for data parallel training.

The main features are:

  - Ease of use: Scale Pytorch's native ``DistributedDataParallel`` and TensorFlow's ``tf.distribute.MirroredStrategy`` without needing to monitor individual nodes.
  - Composibility: RaySGD is built on top of the Ray Actor API, enabling seamless integration with existing Ray applications such as RLlib, Tune, and Ray.Serve.
  - Scale up and down: Start on single CPU. Scale up to multi-node, multi-gpu by changing 2 lines of code.



.. toctree::

   raysgd_pytorch.rst
   raysgd_tensorflow.rst
   raysgd_ft.rst
