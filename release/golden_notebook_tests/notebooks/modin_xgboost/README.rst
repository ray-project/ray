Modin XGBoost-Ray Golden Notebook
=================================

This notebook includes an example workflow using `XGBoost-Ray <https://docs.ray.io/en/latest/xgboost-ray.html>`_
and `Modin <https://modin.readthedocs.io/en/latest/>`_ for distributed model
training and prediction.

To run this notebook in a clean environment, you can run the following:

.. code-block:: bash

    conda create -n modin-xgboost-env python=3.7 pip
    conda activate modin-xgboost-env
    conda install ipykernel
    ipython kernel install --user --name=modin-xgboost-env-kernel
    jupyter notebook
