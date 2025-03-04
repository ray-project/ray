:orphan:

Distributed Training with Hugging Face Accelerate
=================================================

.. raw:: html

    <a id="try-anyscale-quickstart-accelerate_example" target="_blank" href="https://www.anyscale.com/ray-on-anyscale?utm_source=ray_docs&utm_medium=docs&utm_campaign=accelerate_example">
      <img src="../../../_static/img/run-on-anyscale.svg" alt="Run on Anyscale" />
      <br/><br/>
    </a>

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

* :doc:`Ray Train Examples <../../examples>` for more use cases
