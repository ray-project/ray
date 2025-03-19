More Ray ML Libraries
=====================

.. toctree::
    :hidden:

    joblib
    multiprocessing
    ray-collective
    dask-on-ray
    raydp
    mars-on-ray
    modin/index
    Ray Workflows (Deprecated) <../workflows/index>


.. TODO: we added the three Ray Core examples below, since they don't really belong there.
    Going forward, make sure that all "Ray Lightning" and XGBoost topics are in one document or group,
    and not next to each other.

Ray has a variety of additional integrations with ecosystem libraries.

- :ref:`ray-joblib`
- :ref:`ray-multiprocessing`
- :ref:`ray-collective`
- :ref:`dask-on-ray`
- :ref:`spark-on-ray`
- :ref:`mars-on-ray`
- :ref:`modin-on-ray`

.. _air-ecosystem-map:

Ecosystem Map
-------------

The following map visualizes the landscape and maturity of Ray components and their integrations. Solid lines denote integrations between Ray components; dotted lines denote integrations with the broader ML ecosystem.

* **Stable**: This component is stable.
* **Beta**: This component is under development and APIs may be subject to change.
* **Alpha**: This component is in early development.
* **Community-Maintained**: These integrations are community-maintained and may vary in quality.

.. image:: /images/air-ecosystem.svg
