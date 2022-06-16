.. _air-preprocessors:

Inference with trained models
=============================

Once you have a trained model, you can use Ray AIR's Predictors and associated utilities to perform scalable offline and
online prediction.

Creating a Predictor
--------------------
Predictors can be created from Checkpoints.

Either as a result of Training (Result, ResultGrid) or pretrained model.

Link to Checkpoint docs.

Code snippet showing from_checkpoint.

3 ways to use Predictors
------------------------

1: Standalone for development/debugging
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

2: Offline Batch Prediction
~~~~~~~~~~~~~~~~~~~~~~~~~~~
Link to batch prediction guide

3: Online Prediction with Ray Serve
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Link to serving guide


Predictor Inputs and Data formats
---------------------------------
Introduce DataBatchType and supported input types.

Non Deep Learning
~~~~~~~~~~~~~~~~~
Show tabs for xgboost and lightgbm

Link to full batch prediction example
Link to full online serving example

Deep Learning
~~~~~~~~~~~~~
Show tabs for Torch/Tf for each

Single tensor (e.g. Image Data)
###############################
Link to full batch prediction example
Link to full online serving

Tabular
#######
Link to full batch prediction example
Link to full online serving example

Multi-modal
###########
Link to full batch prediction example
Link to full online serving example



