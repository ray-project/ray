:orphan:

Train with DeepSpeed ZeRO-3 and Ray Train
=========================================

.. raw:: html

    <a id="try-anyscale-quickstart-deepspeed_example" target="_blank" href="https://www.anyscale.com/ray-on-anyscale?utm_source=ray_docs&utm_medium=docs&utm_campaign=deepspeed_example">
      <img src="../../../_static/img/run-on-anyscale.svg" alt="Run on Anyscale" />
      <br/><br/>
    </a>

This is an intermediate example that shows how to do distributed training with DeepSpeed ZeRO-3 and Ray Train.
It demonstrates how to use :ref:`Ray Data <data>` with DeepSpeed ZeRO-3 and Ray Train.
If you just want to quickly convert your existing TorchTrainer scripts into Ray Train, you can refer to the :ref:`Train with DeepSpeed <train-deepspeed>`.


Code example
------------

.. literalinclude:: /../../python/ray/train/examples/deepspeed/deepspeed_torch_trainer.py


See also
--------

* :doc:`Ray Train Examples <../../examples>` for more use cases.

* :ref:`Get Started with DeepSpeed <train-deepspeed>` for a tutorial.
