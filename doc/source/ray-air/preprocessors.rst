.. _air-preprocessors:

Preprocessing Data
==================

This page describes how to perform data preprocessing in Ray AIR.

Data preprocessing is a common technique for transforming raw data into features that will be input to a machine learning model.
In general, you may want to apply the same preprocessing logic to your offline training data and online inference data.
Ray AIR provides several common preprocessors out of the box as well as interfaces that enable you to define your own custom logic.

Overview
--------

Ray AIR exposes a ``Preprocessor`` class for preprocessing. The Preprocessor has 4 methods that make up its core interface.

#. ``fit()``: Compute some state for a Dataset and save it to the Preprocessor.
#. ``transform()``: Apply a transformation to a Dataset.
#. ``transform_batch()``: Apply a transformation to a single batch of data.
#. ``fit_transform()``: Syntactic sugar for calling both ``fit()`` and ``transform()`` on a Dataset.

To show these in action, let's walk through a basic example. First we'll set up 2 simple Ray Datasets.

.. literalinclude:: doc_code/preprocessors.py
    :language: python
    :start-after: __preprocessor_setup_start__
    :end-before: __preprocessor_setup_end__

Next, ``fit`` the Preprocessor on one Dataset, and ``transform`` both Datasets with this fitted information.

.. literalinclude:: doc_code/preprocessors.py
    :language: python
    :start-after: __preprocessor_fit_transform_start__
    :end-before: __preprocessor_fit_transform_end__

Finally, call ``transform_batch`` on a single batch of data.

.. literalinclude:: doc_code/preprocessors.py
    :language: python
    :start-after: __preprocessor_transform_batch_start__
    :end-before: __preprocessor_transform_batch_end__

Life of an AIR Preprocessor
---------------------------

Now that we've gone over the basics, let's dive into how Preprocessors fit into an end-to-end application built with AIR.

.. figure:: images/air-preprocessor.svg

Throughout this section, we'll go through an example using XGBoost. The same logic is applicable to other integrations as well.

Trainer
~~~~~~~

The journey of the Preprocessor starts with the ``Trainer``. If the Trainer is instantiated with a Preprocessor,
then the following logic will be executed when ``Trainer.fit()`` is called:

#. If a ``"train"`` Dataset is passed in, then the Preprocessor will call ``fit()`` on it.
#. The Preprocessor will then call ``transform()`` on *all* Datasets, including the ``"train"`` Dataset.
#. The Trainer will then perform training on the preprocessed Datasets.

.. literalinclude:: doc_code/preprocessors.py
    :language: python
    :start-after: __trainer_start__
    :end-before: __trainer_end__

.. note::

    If you're passing a Preprocessor that is already fitted, it will be refitted on the ``"train"`` Dataset.
    Adding the functionality to support passing in a fitted Preprocessor is being tracked
    `here <https://github.com/ray-project/ray/issues/25299>`__.

Tune
~~~~

If you're using ``Ray Tune`` for hyperparameter optimization, be aware that each Trial will instantiate its own copy of
the Preprocessor and the fitting and transformation logic will occur once per Trial.

Checkpoint
~~~~~~~~~~

``Trainer.fit()`` returns a ``Results`` object which contains a ``Checkpoint``.
If a Preprocessor was passed into the Trainer, then it will be saved in the Checkpoint along with any fitted state.

As a sanity check, let's confirm the Preprocessor is available in the Checkpoint. In practice you should not need to do this.

.. literalinclude:: doc_code/preprocessors.py
    :language: python
    :start-after: __checkpoint_start__
    :end-before: __checkpoint_end__


Predictor
~~~~~~~~~

A ``Predictor`` can be constructed from a saved ``Checkpoint``. If the Checkpoint contains a Preprocessor,
then the Preprocessor will be used to call ``transform_batch`` on input batches prior to performing inference.

In the following example, we show the Batch Predictor flow. The same logic applies to the Online Inference flow.

.. literalinclude:: doc_code/preprocessors.py
    :language: python
    :start-after: __predictor_start__
    :end-before: __predictor_end__

Types of Preprocessors
----------------------
Ray AIR provides a handful of Preprocessors that you can use out of the box, and more will be added over time.
`Contributions <https://docs.ray.io/en/master/getting-involved.html>`__ are welcome!

.. tabbed:: Tabular

    #. :class:`Categorizer <ray.ml.preprocessors.Categorizer>`
    #. :class:`FeatureHasher <ray.ml.preprocessors.FeatureHasher>`
    #. :class:`LabelEncoder <ray.ml.preprocessors.LabelEncoder>`
    #. :class:`MaxAbsScaler <ray.ml.preprocessors.MaxAbsScaler>`
    #. :class:`MinMaxScaler <ray.ml.preprocessors.MinMaxScaler>`
    #. :class:`Normalizer <ray.ml.preprocessors.Normalizer>`
    #. :class:`OneHotEncoder <ray.ml.preprocessors.OneHotEncoder>`
    #. :class:`OrdinalEncoder <ray.ml.preprocessors.OrdinalEncoder>`
    #. :class:`PowerTransformer <ray.ml.preprocessors.PowerTransformer>`
    #. :class:`RobustScaler <ray.ml.preprocessors.RobustScaler>`
    #. :class:`SimpleImputer <ray.ml.preprocessors.SimpleImputer>`
    #. :class:`StandardScaler <ray.ml.preprocessors.StandardScaler>`
    #. :class:`SimpleImputer <ray.ml.preprocessors.SimpleImputer>`

.. tabbed:: Text

    #. :class:`CountVectorizer <ray.ml.preprocessors.CountVectorizer>`
    #. :class:`HashingVectorizer <ray.ml.preprocessors.HashingVectorizer>`
    #. :class:`Tokenizer <ray.ml.preprocessors.Tokenizer>`

.. tabbed:: Image

    Coming soon!

.. tabbed:: Common APIs

    #. :class:`Preprocessor <ray.ml.preprocessor.Preprocessor>`
    #. :class:`BatchMapper <ray.ml.preprocessors.BatchMapper>`
    #. :class:`Chain <ray.ml.preprocessors.Chain>`

.. tabbed:: Utilities

    #. :func:`train_test_split <ray.ml.train_test_split>`

Chain
~~~~~

More often than not, your preprocessing logic will contain multiple logical steps or apply different transformations to each column.
A simple ``Chain`` Preprocessor is provided which can be used to apply individual Preprocessor operations sequentially.

.. literalinclude:: doc_code/preprocessors.py
    :language: python
    :start-after: __chain_start__
    :end-before: __chain_end__

.. tip::

    Keep in mind that the operations are sequential. For example, if you define a Preprocessor
    ``Chain([preprocessorA, preprocessorB])``, then ``preprocessorB.transform()`` will be applied
    to the result of ``preprocessorA.transform()``.

Custom preprocessors
~~~~~~~~~~~~~~~~~~~~

**Stateless Preprocessors:** Stateless preprocessors can be implemented with the ``BatchMapper``.

.. literalinclude:: doc_code/preprocessors.py
    :language: python
    :start-after: __custom_stateless_start__
    :end-before: __custom_stateless_end__

**Stateful Preprocessors:** Coming soon!

