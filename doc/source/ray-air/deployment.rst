Deploying Ray for ML platforms
==============================

This page describes how you might use or deploy Ray in your infrastructure. There are two main deployment patterns---pick and choose, and within existing platforms.

The core idea is that Ray can be **complementary** to your existing infrastructure and integration tools.

Design Principles
-----------------

* Ray and its libraries handle the heavyweight compute aspects of AI apps and services.
* Ray relies on external integrations (for example, Tecton, MLflow, W&B) for Storage and Tracking.
* Workflow Orchestrators (for example, AirFlow) are an optional component that can be used for scheduling recurring jobs, launching new Ray clusters for jobs, and running non-Ray compute steps.
* Lightweight orchestration of task graphs within a single Ray app can be handled using Ray tasks.
* Ray libraries can be used independently, within an existing ML platform, or to build a Ray-native ML platform.


Pick and choose your own libraries
-----------------------------------

You can pick and choose which Ray AI libraries you want to use.

This is applicable if you're an ML engineer who wants to independently use a Ray library for a specific AI app or service use case and don't need to integrate with existing ML platforms.

For example, Alice wants to use RLlib to train models for her work project. Bob wants to use Ray Serve to deploy his model pipeline. Both Alice and Bob can leverage these libraries independently without any coordination.

This scenario describes most usages of Ray libraries today.

.. https://docs.google.com/drawings/d/1DcrchNda9m_3MH45NuhgKY49ZCRtj2Xny5dgY0X9PCA/edit

.. image:: /images/air_arch_1.svg

In the preceding diagram:

* Only one library is used---showing that you can pick and choose and don't need to replace all of your ML infrastructure to use Ray.
* You can use one of :ref:`Ray's many deployment modes <jobs-overview>` to launch and manage Ray clusters and Ray applications.
* Ray AI libraries can read data from external storage systems such as Amazon S3 / Google Cloud Storage, as well as store results there.



Existing ML Platform integration
--------------------------------

You may already have an existing machine learning platform but want to use some subset of Ray's ML libraries. For example, an ML engineer wants to use Ray within the ML Platform their organization has purchased (for example, SageMaker, Vertex).

Ray can complement existing machine learning platforms by integrating with existing pipeline/workflow orchestrators, storage, and tracking services, without requiring a replacement of your entire ML platform.


.. image:: images/air_arch_2.png


In the preceding diagram:

1. A workflow orchestrator such as AirFlow, Oozie, SageMaker Pipelines, etc. is responsible for scheduling and creating Ray clusters and running Ray apps and services. The Ray application may be part of a larger orchestrated workflow (for example, Spark ETL, then Training on Ray).
2. Lightweight orchestration of task graphs can be handled entirely within Ray. External workflow orchestrators integrate nicely but are only needed if running non-Ray steps.
3. Ray clusters can also be created for interactive use (for example, Jupyter notebooks, Google Colab, Databricks Notebooks, etc.).
4. Ray Train, Data, and Serve provide integration with Feature Stores like Feast for Training and Serving.
5. Ray Train and Tune provide integration with tracking services such as MLflow and Weights & Biases.
