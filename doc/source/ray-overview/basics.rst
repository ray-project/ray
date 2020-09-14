
.. image:: https://github.com/ray-project/ray/raw/master/doc/source/images/ray_header_logo.png

**Ray provides a simple, universal API for building distributed applications.**

Ray accomplishes this mission by:

1. Providing simple primitives for building and running distributed applications.
2. Enabling end users to parallelize single machine code, with little to zero code changes.
3. Including a large ecosystem of applications, libraries, and tools on top of the core Ray to enable complex applications.

**Ray Core** provides the simple primitives for application building.

On top of **Ray Core** are several libraries for solving problems in machine learning:

- :doc:`../tune/index`
- :ref:`rllib-index`
- :ref:`sgd-index`
- :ref:`rayserve`

There are also many :ref:`community libraries <ray-oss-list>` that integrate with Ray, including `Modin`_, `Hugging Face Transformers`_, :doc:`../dask-on-ray`, `MARS`_, `Horovod`_, and others.
Check out Ray's :ref:`rapidly growing list of open source projects <ray-oss-list>`!

.. _`Modin`: https://github.com/modin-project/modin
.. _`Hugging Face Transformers`: https://huggingface.co/transformers/main_classes/trainer.html#transformers.Trainer.hyperparameter_search
.. _`MARS`: https://github.com/mars-project/mars/pull/1508
.. _`Horovod`: https://horovod.readthedocs.io/en/stable/ray_include.html
