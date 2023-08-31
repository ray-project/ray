.. _ray-for-ml-infra:

Ray for ML Infrastructure
=========================

.. tip::

    We'd love to hear from you if you are using Ray to build a ML platform! Fill out `this short form <https://forms.gle/wCCdbaQDtgErYycT6>`__ to get involved.

Ray and its AI Runtime libraries provide unified compute runtime for teams looking to simplify their ML platform.
Ray's libraries such as Ray Train, Ray Data, and Ray Serve can be used to compose end-to-end ML workflows, providing features and APIs for
data preprocessing as part of training, and transitioning from training to serving.

..
  https://docs.google.com/drawings/d/1atB1dLjZIi8ibJ2-CoHdd3Zzyl_hDRWyK2CJAVBBLdU/edit

.. image:: /images/ray-air.svg

Why Ray for ML Infrastructure?
------------------------------

Ray's AI Runtime libraries simplify the ecosystem of machine learning frameworks, platforms, and tools, by providing a seamless, unified, and open experience for scalable ML:


.. image:: images/why-air-2.svg

..
  https://docs.google.com/drawings/d/1oi_JwNHXVgtR_9iTdbecquesUd4hOk0dWgHaTaFj6gk/edit

**1. Seamless Dev to Prod**: Ray's AI Runtime libraries reduces friction going from development to production. With Ray and its libraries, the same Python code scales seamlessly from a laptop to a large cluster.

**2. Unified ML API and Runtime**: Ray's APIs enables swapping between popular frameworks, such as XGBoost, PyTorch, and Hugging Face, with minimal code changes. Everything from training to serving runs on a single runtime (Ray + KubeRay).

**3. Open and Extensible**: Ray is fully open-source and can run on any cluster, cloud, or Kubernetes. Build custom components and integrations on top of scalable developer APIs.

Example ML Platforms built on Ray
---------------------------------

`Merlin <https://shopify.engineering/merlin-shopify-machine-learning-platform>`_ is Shopify's ML platform built on Ray. It enables fast-iteration and `scaling of distributed applications <https://www.youtube.com/watch?v=kbvzvdKH7bc>`_ such as product categorization and recommendations.

.. figure:: /images/shopify-workload.png

  Shopify's Merlin architecture built on Ray.

Spotify `uses Ray for advanced applications <https://www.anyscale.com/ray-summit-2022/agenda/sessions/180>`_ that include personalizing content recommendations for home podcasts, and personalizing Spotify Radio track sequencing.

.. figure:: /images/spotify.png

  How Ray ecosystem empowers ML scientists and engineers at Spotify..


The following highlights feature companies leveraging Ray's unified API to build simpler, more flexible ML platforms.

- `[Blog] The Magic of Merlin - Shopify's New ML Platform <https://shopify.engineering/merlin-shopify-machine-learning-platform>`_
- `[Slides] Large Scale Deep Learning Training and Tuning with Ray <https://drive.google.com/file/d/1BS5lfXfuG5bnI8UM6FdUrR7CiSuWqdLn/view>`_
- `[Blog] Griffin: How Instacart’s ML Platform Tripled in a year <https://www.instacart.com/company/how-its-made/griffin-how-instacarts-ml-platform-tripled-ml-applications-in-a-year/>`_
- `[Talk] Predibase - A low-code deep learning platform built for scale <https://www.youtube.com/watch?v=B5v9B5VSI7Q>`_
- `[Blog] Building a ML Platform with Kubeflow and Ray on GKE <https://cloud.google.com/blog/products/ai-machine-learning/build-a-ml-platform-with-kubeflow-and-ray-on-gke>`_
- `[Talk] Ray Summit Panel - ML Platform on Ray <https://www.youtube.com/watch?v=_L0lsShbKaY>`_


.. Deployments on Ray.
.. include:: /ray-air/deployment.rst
