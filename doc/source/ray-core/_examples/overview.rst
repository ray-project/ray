Ray Tutorials and Examples
==========================

Get started with Ray, Tune, and RLlib with these notebooks that you can run online in Colab or Binder: `Ray Tutorial Notebooks <https://github.com/ray-project/tutorial>`__


Ray Examples
------------

.. raw:: html

    <div class="sphx-glr-bigcontainer">

.. toctree::
   :hidden:

   tips-for-first-time.rst
   testing-tips.rst
   progress_bar.rst
   plot_streaming.rst
   placement-group.rst

.. customgalleryitem::
   :tooltip: Tips for first time users.
   :figure: /images/pipeline.png
   :description: :doc:`tips-for-first-time`

.. customgalleryitem::
   :tooltip: Tips for testing Ray applications
   :description: :doc:`testing-tips`

.. customgalleryitem::
   :tooltip: Progress Bar for Ray Tasks
   :description: :doc:`progress_bar`

.. customgalleryitem::
   :tooltip: Implement a simple streaming application using Ray’s actors.
   :description: :doc:`plot_streaming`

.. customgalleryitem::
   :tooltip: Learn placement group use cases with examples.
   :description: :doc:`placement-group`

.. raw:: html

    </div>


Machine Learning Examples
-------------------------

.. raw:: html

    <div class="sphx-glr-bigcontainer">

.. toctree::
   :hidden:

   plot_parameter_server.rst
   plot_hyperparameter.rst
   plot_lbfgs.rst
   plot_example-lm.rst
   plot_newsreader.rst
   dask_xgboost/dask_xgboost.rst
   modin_xgboost/modin_xgboost.rst


.. customgalleryitem::
   :tooltip: Build a simple parameter server using Ray.
   :figure: /ray-core/images/param_actor.png
   :description: :doc:`plot_parameter_server`

.. customgalleryitem::
   :tooltip: Simple parallel asynchronous hyperparameter evaluation.
   :figure: /ray-core/images/hyperparameter.png
   :description: :doc:`plot_hyperparameter`

.. customgalleryitem::
   :tooltip: Walkthrough of parallelizing the L-BFGS algorithm.
   :description: :doc:`plot_lbfgs`


.. customgalleryitem::
   :tooltip: Distributed Fault-Tolerant BERT training for FAIRSeq using Ray.
   :description: :doc:`plot_example-lm`

.. customgalleryitem::
   :tooltip: Implementing a simple news reader using Ray.
   :description: :doc:`plot_newsreader`

.. customgalleryitem::
   :tooltip: Train an XGBoost-Ray model using Dask for data processing.
   :description: :doc:`dask_xgboost/dask_xgboost`

.. customgalleryitem::
   :tooltip: Train an XGBoost-Ray model using Modin for data processing.
   :description: :doc:`modin_xgboost/modin_xgboost`


.. raw:: html

    </div>


Reinforcement Learning Examples
-------------------------------

These are simple examples that show you how to leverage Ray Core. For Ray's production-grade reinforcement learning library, see `RLlib <http://docs.ray.io/en/latest/rllib.html>`__.

.. raw:: html

    <div class="sphx-glr-bigcontainer">

.. toctree::
   :hidden:

   plot_pong_example.rst
   plot_example-a3c.rst

.. customgalleryitem::
   :tooltip: Asynchronous Advantage Actor Critic agent using Ray.
   :figure: /ray-core/images/a3c.png
   :description: :doc:`plot_example-a3c`

.. customgalleryitem::
   :tooltip: Parallelizing a policy gradient calculation on OpenAI Gym Pong.
   :figure: /images/pong.png
   :description: :doc:`plot_pong_example`

.. raw:: html

    </div>

End-to-end Machine Learning Guides
----------------------------------

These are full guides on how you can use Ray with various Machine Learning libraries

.. raw:: html

    <div class="sphx-glr-bigcontainer">

.. toctree::
   :hidden:

   using-ray-with-pytorch-lightning.rst

.. customgalleryitem::
   :tooltip: Using Ray with PyTorch Lightning.
   :figure: /images/pytorch_lightning_small.png
   :description: :doc:`using-ray-with-pytorch-lightning`
