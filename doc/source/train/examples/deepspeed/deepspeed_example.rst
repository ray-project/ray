:orphan:

.. _deepspeed_example:

Train with DeepSpeed ZeRO-3 and Ray Train
=========================================

This is an intermediate example that shows how to do distributed training with DeepSpeed ZeRO-3 and Ray Train.
It demonstrates how to use :ref:`Ray Dataset <data>` with DeepSpeed ZeRO-3 and Ray Train.
If you just want to quickly convert your existing TorchTrainer scripts into Ray Train, you can refer to the :ref:`Train with DeepSpeed <train-deepspeed>`.


Code example
------------

.. literalinclude:: /../../python/ray/train/examples/deepspeed/deepspeed_torch_trainer.py


See also
--------

* :ref:`Ray Train Examples <train-examples>` for more use cases.

* :ref:`Get Started with DeepSpeed <train-horovod>` for a tutorial.    
