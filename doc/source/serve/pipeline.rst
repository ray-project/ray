.. _serve-pipeline-api:

Pipeline API (Experimental)
===========================

This section should help you:

- understand the experimental pipeline API.
- build on top of the API to construct your multi-model inference pipelines.


.. note::
    This API is experimental and the API is subject to change.
    We are actively looking for feedback via the Ray `Forum`_ or `GitHub Issues`_

Serve Pipeline is a new experimental package purposely built to help developing
and deploying multi-models inference pipelines, also known as model composition.

Model composition is common in real-world ML applications. In many cases, you need to:

- Split CPU bounded preprocessing and GPU bounded model inference to scale each phase separately.
- Chain multiple models together for a single tasks.
- Combine the output from multiple models to create ensemble result.
- Dynamically select models based on attribute of the input data.

The Serve Pipeline has the following features:

- It has a python based, declarative API for constructing pipeline DAG.
- It gives you visibility into the whole pipeline without losing the flexibility
  of coding arbitrary graph using code.
- You can develop and test pipeline locally with local execution mode.
- Each model in the DAG can be scaled to many replicas across the Ray cluster.
  You can fine-tune the resource usage to achieve maximum throughput and utilization.

Compared to ServeHandle, Serve Pipeline is more explicit about the dependencies
of each model in the pipeline and let you deploy the entire DAG at once.

Compared to KServe (formerly KFServing), Serve Pipeline enables writing pipeline
as code and arbitrary control flow operation using Python.

Compared to building your own orchestration micro-services, Serve Pipeline helps
you to be productive in building scalable pipeline in hours.


Chaining Example
----------------

In this section, we show how to implement a two stage pipeline that's common
for computer vision tasks. For workloads like image classification, the preprocessing
steps are CPU bounded and hard to parallelize. The actual inference steps can run
on GPU and batched (batching helps improving throughput without sacrificing latency,
you can learn more in our :ref:`batching tutorial <serve-batch-tutorial>`).
They are often split up into separate stages and scaled separately to increase throughput.


.. _serve-pipeline-ensemble-api:


Ensemble Example
----------------

We will now expand on previous example to construct an ensemble pipeline. In
the previous example, our pipeline looks like: preprocess -> resnet18. What if we
want to aggregate the output from many different models? You can build this scatter-gather
pattern easily with Pipeline. The below code snippet shows how to construct a pipeline
looks like: preprocess -> [resnet18, resnet34] -> combine_output.


More Use Case Examples
----------------------

There are even more use cases for Serve Pipeline.

.. note::
    Please feel free to suggest more use cases and contribute your examples by
    sending a `Github Pull Requests`_!

Combining business logic + ML models
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Based off the previous ensemble example, you can put arbitrary business logic
in your pipeline step.


.. _`Forum`: https://discuss.ray.io/
.. _`GitHub Issues`: https://github.com/ray-project/ray/issues
.. _`GitHub Pull Requests`: https://github.com/ray-project/ray/pulls
