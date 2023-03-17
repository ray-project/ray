.. _tune-fault-tolerance:

How to Enable Fault Tolerance in Ray Tune
=========================================

Fault tolerance is an important feature for distributed machine learning experiments that can help mitigate the impact of node failures due to out of memory and out of disk issues. With fault tolerance, users can save time and resources by preserving the training progress even if a node fails. In this guide, we will cover how to enable fault tolerance in Ray Tune using the open-source Ray Tune library.

Experiment-level Fault Tolerance
--------------------------------

At the experiment level, users can use the Tuner.restore method to resume training from a previously interrupted Tune experiment. This covers cases where the Tune driver script that calls Tuner.fit() errors out. To enable experiment-level fault tolerance, simply specify the Tuner.restore method with the path to the checkpoint directory:


Trial-level Fault Tolerance
---------------------------

Trial-level fault tolerance deals with individual node failures in the cluster. Ray Tune provides a way to configure a FailureConfig object with the number of retries per trial, as well as trial checkpointing.


Example with Tune Experiment-level and Trial-level Fault Tolerance
------------------------------------------------------------------