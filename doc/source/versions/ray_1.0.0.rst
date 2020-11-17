.. _ray1.0.0:

Ray 1.0.0 Release Notes
=======================

Ray Core
--------

- The ray.init() and ``ray start`` commands have been cleaned up to remove deprecated arguments
- The Ray Java API is now stable
- Improved detection of Docker CPU limits 
- Add support and documentation for Dask-on-Ray and MARS-on-Ray: https://docs.ray.io/en/master/ray-libraries.html
- Placement groups for fine-grained control over scheduling decisions: https://docs.ray.io/en/latest/placement-group.html.
- New architecture whitepaper: https://docs.ray.io/en/master/whitepaper.html

Autoscaler
~~~~~~~~~~

- Support for multiple instance types in the same cluster: https://docs.ray.io/en/master/cluster/autoscaling.html
- Support for specifying GPU/accelerator type in ``@ray.remote``

Dashboard & Metrics
~~~~~~~~~~~~~~~~~~~

- Improvements to the memory usage tab and machine view
- The dashboard now supports visualization of actor states
- Support for Prometheus metrics reporting: https://docs.ray.io/en/latest/ray-metrics.html

RLlib
-----

- Two Model-based RL algorithms were added: MB-MPO ("Model-based meta-policy optimization") and "Dreamer". Both algos were benchmarked and are performing comparably to the respective papers' reported results.
- A "Curiosity" (intrinsic motivation) module was added via RLlib's Exploration API and benchmarked on a sparse-reward Unity3D environment (Pyramids).
- Added documentation for the Distributed Execution API.
- Removed (already soft-deprecated) APIs: Model(V1) class, Trainer config keys, some methods/functions. Where you would see a warning previously when using these, there will be an error thrown now.
- Added DeepMind Control Suite examples.

Tune
----

**Breaking changes:**
-  Multiple tune.run parameters have been deprecated: ``ray_auto_init, run_errored_only, global_checkpoint_period, with_server`` (#10518)
- ``tune.run(upload_dir, sync_to_cloud, sync_to_driver, sync_on_checkpoint`` have been moved to ``tune.SyncConfig`` [[docs](https://docs.ray.io/en/releases-1.0.0/tune/tutorials/tune-distributed.html#syncing)] (#10518)

**New APIs:**
- ``mode, metric, time_budget`` parameters for tune.run (#10627, #10642)
- Search Algorithms now share a uniform API: (#10621, #10444). You can also use the new ``create_scheduler/create_searcher`` shim layer to create search algorithms/schedulers via string, reducing boilerplate code (#10456).
- Native callbacks for: [MXNet, Horovod, Keras, XGBoost, PytorchLightning](https://docs.ray.io/en/releases-1.0.0/tune/api_docs/integration.html) (#10533, #10304, #10509, #10502, #10220)
- PBT runs can be replayed with PopulationBasedTrainingReplay scheduler (#9953)
- Search Algorithms are saved/resumed automatically (#9972)
- New Optuna Search Algorithm [docs](https://docs.ray.io/en/releases-1.0.0/tune/api_docs/suggestion.html#optuna-tune-suggest-optuna-optunasearch) (#10044)
- Tune now can sync checkpoints across Kubernetes pods (#10097)
- Failed trials can be rerun with `tune.run(resume="run_errored_only")` (#10060)

**Other Changes:**
- Trial outputs can be saved to file via ``tune.run(log_to_file=...)`` (#9817)
- Trial directories can be customized, and default trial directory now includes trial name (#10608, #10214)
- Improved Experiment Analysis API (#10645)
- Support for Multi-objective search via SigOpt Wrapper (#10457, #10446)
- BOHB Fixes (#10531, #10320)
- Wandb improvements + RLlib compatibility (#10950, #10799, #10680, #10654, #10614, #10441, #10252, #8521)
- Updated documentation for FAQ, Tune+serve, search space API, lifecycle (#10813, #10925, #10662, #10576, #9713, #10222, #10126, #9908)


RaySGD
------

* Creator functions are subsumed by the TrainingOperator API (#10321)
* Training happens on actors by default (#10539)

Serve
-----

- [``serve.client`` API](https://docs.ray.io/en/master/serve/deployment.html#lifetime-of-a-ray-serve-instance) makes it easy to appropriately manage lifetime for multiple Serve clusters. (#10460) 
- Serve APIs are fully typed. (#10205, #10288)
- Backend configs are now typed and validated via Pydantic. (#10559, #10389)
- Progress towards application level backend autoscaler. (#9955, #9845, #9828)
- New [architecture page](https://docs.ray.io/en/master/serve/architecture.html) in documentation. (#10204)
