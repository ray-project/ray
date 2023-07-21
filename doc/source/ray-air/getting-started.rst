:orphan:

Ray AI Runtime (AIR)
====================

.. tip::

    We'd love to hear from you if you are using Ray to build a ML platform! Fill out `this short form <https://forms.gle/wCCdbaQDtgErYycT6>`__ to get involved.

Ray AI Runtime (AIR) is a scalable and unified toolkit for ML applications. 
AIR builds on Ray's best-in-class ML libraries such as Ray Train, Ray Data, and Ray Serve to connect workflows together, providing features and APIs for
data preprocessing as part of training, and transitioning from training to serving.

..
  https://docs.google.com/drawings/d/1atB1dLjZIi8ibJ2-CoHdd3Zzyl_hDRWyK2CJAVBBLdU/edit

.. image:: images/ray-air.svg


ML Compute, Simplified
----------------------

Ray AIR aims to simplify the ecosystem of machine learning frameworks, platforms, and tools. It does this by leveraging Ray and its libraries to provide a seamless, unified, and open experience for scalable ML:

.. image:: images/why-air-2.svg

..
  https://docs.google.com/drawings/d/1oi_JwNHXVgtR_9iTdbecquesUd4hOk0dWgHaTaFj6gk/edit

**1. Seamless Dev to Prod**: AIR reduces friction going from development to production. With Ray and AIR, the same Python code scales seamlessly from a laptop to a large cluster.

**2. Unified ML API**: AIR's unified ML API enables swapping between popular frameworks, such as XGBoost, PyTorch, and Hugging Face, with just a single class change in your code.

**3. Open and Extensible**: AIR and Ray are fully open-source and can run on any cluster, cloud, or Kubernetes. Build custom components and integrations on top of scalable developer APIs.

When to use AIR?
----------------

AIR is for both data scientists and ML engineers alike.

.. image:: images/when-air.svg

..
  https://docs.google.com/drawings/d/1Qw_h457v921jWQkx63tmKAsOsJ-qemhwhCZvhkxWrWo/edit

For data scientists, AIR can be used to scale individual workloads, and also end-to-end ML applications. For ML Engineers, AIR provides scalable platform abstractions that can be used to easily onboard and integrate tooling from the broader ML ecosystem.
