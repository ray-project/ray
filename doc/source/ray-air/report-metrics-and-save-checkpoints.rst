.. _report_metrics_and_save_checkpoints:

Reporting metrics and saving checkpoint
=======================================

Overview
--------
Regardless of training or tuning, one often needs to report metrics and save checkpoints for
fault tolerance or future reference. This can be challenging in a distributed environment, where
the calculation of metrics and the generation of checkpoints are spread out across multiple nodes
in a cluster.

Ray AIR has made this extremely simple by exposing the :ref:`Session <air-session-ref>` API.
Under the hood, this API makes sure that metrics are presented in the final training or tuning result. And
checkpoints are synced to driver or the cloud storage based on user's configurations.

Take a look at the following code snippet.

.. literalinclude:: doc_code/report_metrics_and_save_checkpoints.py
    :language: python
    :start-after: __air_session_start__
    :end-before: __air_session_end__

What's happening under the hood (advanced concept)
--------------------------------------------------
If you are tuning a non distributed data parallel (DDP) training function, you are only using Tune session.
There is one Tune session per training function.
If you are doing DDP training or tuning multiple trials, each being a DDP training job,
then you are using both Tune session and Train session under the hood.
There is one Tune session per trial and one Train session per DDP worker.

In general, the session concept exists on several levels:
The execution layer (called `Tune Session`) and the Data Parallel training layer
(called `Train Session`).
The following figure shows how these two sessions look like in a Data Parallel training scenario.

.. image:: images/session.svg
   :width: 650px
   :align: center

..
  https://docs.google.com/drawings/d/1g0pv8gqgG29aPEPTcd4BC0LaRNbW1sAkv3H6W1TCp0c/edit
