
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
- :ref:`datasets` (beta)
- :ref:`workflows` (alpha)

There are also many :ref:`community integrations <ray-oss-list>` with Ray, including `Dask`_, `MARS`_, `Modin`_, `Horovod`_, `Hugging Face`_, `Scikit-learn`_, and others. Check out the :ref:`full list of Ray distributed libraries here <ray-oss-list>`.

.. _`Modin`: https://github.com/modin-project/modin
.. _`Hugging Face`: https://huggingface.co/transformers/main_classes/trainer.html#transformers.Trainer.hyperparameter_search
.. _`MARS`: https://docs.ray.io/en/latest/data/mars-on-ray.html
.. _`Dask`: https://docs.ray.io/en/latest/data/dask-on-ray.html
.. _`Horovod`: https://horovod.readthedocs.io/en/stable/ray_include.html
.. _`Scikit-learn`: joblib.html
