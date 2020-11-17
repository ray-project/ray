.. _ray1.0.1:

Ray 1.0.1 Release Notes
=======================

Highlights
----------

* If you're migrating from Ray < 1.0.0, be sure to check out the [1.0 Migration Guide](https://github.com/ray-project/ray/discussions/11482).
* Autoscaler is now **docker by default**.
* RLLib features multiple new environments.
* Tune supports population based bandits, checkpointing in Docker, and multiple usability improvements. 
* SGD supports PyTorch Lightning 
* All of Ray's components and libraries have improved performance, scalability, and stability.

Core
----
* [1.0 Migration Guide](https://github.com/ray-project/ray/discussions/11482).
* Many bug fixes and optimizations in GCS.
* Polishing of the Placement Group API. 
* Improved Java language support

RLlib
-----
* Added documentation for Curiosity exploration module (#11066).
* Added RecSym environment wrapper (#11205).
* Added Kaggle’s football environment (multi-agent) wrapper (#11249).
* Multiple bug fixes: GPU related fixes for SAC (#11298), MARWIL, all example scripts run on GPU (#11105), lifted limitation on 2^31 timesteps (#11301), fixed eval workers for ES and ARS (#11308), fixed broken no-eager-no-workers mode (#10745).
* Support custom MultiAction distributions (#11311).
* No environment is created on driver (local worker) if not necessary (#11307).
* Added simple SampleCollector class for Trajectory View API (#11056).
* Code cleanup: Docstrings and type annotations for Exploration classes (#11251), DQN (#10710), MB-MPO algorithm, SAC algorithm (#10825).

Serve
-----
* API: Serve will error when ``serve_client`` is serialized. (#11181)
* Performance: ``serve_client.get_handle("endpoint")`` will now get a handle to nearest node, increasing scalability in distributed mode. (#11477)
* Doc: Added FAQ page and updated architecture page (#10754, #11258)
* Testing: New distributed tests and benchmarks are added (#11386)
* Testing: Serve now run on Windows (#10682)

SGD
---
* Pytorch Lightning integration is now supported (#11042)
* Support ``num_steps`` continue training (#11142)
* Callback API for SGD+Tune  (#11316)

Tune
----
* New Algorithm: Population-based Bandits (#11466)
* ``tune.with_parameters()``, a wrapper function to pass arbitrary objects through the object store to trainables (#11504)
* Strict metric checking - by default, Tune will now error if a result dict does not include the optimization metric as a key. You can disable this with TUNE_DISABLE_STRICT_METRIC_CHECKING (#10972)
* Syncing checkpoints between multiple Docker containers on a cluster is now supported with the ``DockerSyncer``  (#11035)
* Added type hints (#10806)
* Trials are now dynamically created (instead of created up front) (#10802)
* Use ``tune.is_session_enabled()`` in the Function API to toggle between Tune and non-tune code  (#10840)
* Support hierarchical search spaces for hyperopt (#11431)
* Tune function API now also supports ``yield`` and ``return`` statements (#10857)
* Tune now supports callbacks with ``tune.run(callbacks=...`` (#11001)
* By default, the experiment directory will be dated (#11104)
* Tune now supports ``reuse_actors`` for function API, which can largely accelerate tuning jobs.

