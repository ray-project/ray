:orphan:

.. _accelerate_example:

Distributed Training with Hugging Face Accelerate
=================================================

This example does distributed data parallel training
with Hugging Face Accelerate, Ray Train, and Ray Data.
It fine-tunes a BERT model and is adapted from
https://github.com/huggingface/accelerate/blob/main/examples/nlp_example.py


Code example
------------

.. literalinclude:: /../../python/ray/train/examples/accelerate/accelerate_torch_trainer.py

See also
--------

* :ref:`Get Started with Hugging Face Accelerate <train-hf-accelerate>` for a tutorial on using Ray Train and HF Accelerate

* :ref:`Ray Train Examples <train-examples>` for more use cases
