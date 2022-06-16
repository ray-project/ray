Batch Prediction
================

Ray AIR provides a ``BatchPredictor`` utility for large-scale batch inference.

First, familiarize yourself with :ref:`Predictors <air-predictors>`.

Creating a ``BatchPredictor``
-----------------------------
Show Code Snippet of from_checkpoint

Predicting on a Ray Dataset
---------------------------
:tip: First use :ref:`Predictor standalone <_air-predictor-standalone>` with a sample data batch to make sure the Predictor works with your model/checkpoint.

Link to creating a Ray Dataset.

Link to Dataset creation docs for
1. Images
2. Tabular
3. Multi-modal

Show code snippet ``.predict()`` method

Lazy/Pipelined Prediction
~~~~~~~~~~~~~~~~~~~~~~~~~
Show ``.predict_pipelined()`` method.

Describe a use case.

Configuring Batch Prediction
----------------------------

**Resources (enabling GPU Prediction)**

**Batch Size**

**Worker Pool**


