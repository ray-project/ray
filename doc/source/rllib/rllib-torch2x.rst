.. include:: /_includes/rllib/we_are_hiring.rst

.. include:: /_includes/rllib/new_api_stack.rst


Using RLlib with torch 2.x compile
==================================

torch 2.x comes with the ``torch.compile()`` `API <https://pytorch.org/docs/stable/generated/torch.compile.html#torch.compile>`_, which can be used to JIT-compile wrapped code. We integrate ``torch.compile()`` with RLlib in the context of `RLModules <rllib-rlmodule.html>`_ and Learners.

We have integrated this feature with RLModules. You can set the backend and mode via ``framework()`` API on an :py:class:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig` object. Alternatively, you can compile the :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule` directly during stand-alone usage, such as inference.

Benchmarks
==========

We conducted a comprehensive benchmark with this feature. The following benchmarks consider only the potential speedups due to enabling torch-compile during inference and environment explorations. This speedup method is relevant because RL is usually bottlenecked by sampling.

Inference
---------
For the benchmarking metric, we compute the inverse of the time it takes to run :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule.forward_exploration` of the RLModule. We have conducted this benchmark on the default implementation of PPO RLModule under different hardware settings, torch versions, dynamo backends and modes, as well as different batch sizes. The following table shows the combinations of torch-backend and -mode that yield the highest speedup we could find for a given combination of hardware and PyTorch version:

.. list-table::
   :widths: 25 25 25 25
   :header-rows: 1

   * - Hardware
     - PyTorch Version
     - Speedup (%)
     - Backend + Mode
   * - CPU
     - 2.0.1
     - 33.92
     - ipex + default
   * - CPU
     - 2.1.0 nightly
     - x
     - ipex + default
   * - T4
     - 2.0.1
     - 14.05
     - inductor + reduce-overhead
   * - T4
     - 2.1.0 nightly
     - 15.01
     - inductor + reduce-overhead
   * - V100
     - 2.0.1
     - 92.43
     - inductor + reduce-overhead
   * - V100
     - 2.1.0 nightly
     - 85.71
     - inductor + reduce-overhead
   * - A100
     - 2.0.1
     - x
     - inductor + reduce-overhead
   * - A100
     - 2.1.0 nightly
     - 156.66
     - inductor + reduce-overhead


For detailed tables, see `Appendix <../../../../rllib/benchmarks/torch_compile/README.md#appendix>`_. For the benchmarking code, see `run_inference_bm.py <../../../../rllib/benchmarks/torch_compile/run_inference_bm.py>`_. To run the benchmark use the following command:


.. code-block:: bash

  python ./run_inference_bm.py --backend <dynamo_backend> --mode <dynamo_mode> -bs <batch_size>

Some meta-level comments
########################
1. The performance improvement depends on many factors, including the neural network architecture used, the batch size during sampling, the backend, the mode, the torch version, and many other factors. To optimize performance, get the non-compiled workload learning and then do a hyper-parameter tuning on torch compile parameters on different hardware.

2. For CPU inference use the recommended inference-only backends: ``ipex`` and ``onnxrt``.

3. The speedups are more significant on more modern architectures such as A100s compared to older ones like T4.

4. Torch compile is still evolving. We noticed significant differences between the 2.0.1 release and the 2.1 nightly release. Therefore, it is important to take the torch release  into account during benchmarking your own workloads.

Exploration
------------
In RLlib, you can now set the configuration so that it uses the compiled module during sampling of an RL agent training process. By default, the rollout workers run on CPU, therefore it's recommended to use the ``ipex`` or ``onnxrt`` backend. However, you can still run the sampling part on GPUs as well by setting ``num_gpus_per_env_runner`` in which case other backends can be used as well. For enabling torch-compile during training you can also set `torch_compile_learner` equivalents.



.. testcode::

    from ray.rllib.algorithms.ppo import PPOConfig
    config = PPOConfig().framework(
        "torch",
        torch_compile_worker=True,
        torch_compile_worker_dynamo_backend="ipex",
        torch_compile_worker_dynamo_mode="default",
    )


`This <../../../../rllib/benchmarks/torch_compile/run_ppo_with_inference_bm.py>`_ benchmark script runs the PPO algorithm with the default model architecture for the Atari-Breakout game. It runs the training for ``n`` iterations for both compiled and non-compiled RLModules and reports the speedup. Note that negative speedup values mean a slowdown when you compile the module.

To run the benchmark script, you need a Ray cluster comprised of at least 129 CPUs (2x64 + 1) and 2 GPUs. If this configuration isn't accessible to you, you can change the number of sampling workers and batch size to make the requirements smaller.

.. code-block:: bash

  python ./run_ppo_with_inference_bm.py --backend <backend> --mode <mode>


Here is a summary of results:

.. list-table::
   :widths: 33 33 33
   :header-rows: 1

   * - Backend
     - Mode
     - Speedup (%)
   * - onnxrt
     - default
     - -72.34
   * - onnxrt
     - reduce-overhead
     - -72.72
   * - ipex
     - default
     - 11.71
   * - ipex
     - reduce-overhead
     - 11.31
   * - ipex
     - max-autotune
     - 12.88

As you can see, ``onnxrt`` does not gain any speedups in the setup we tested (in fact it slows the workload down by 70%), while the ``ipex`` provides ~10% speedup. If we change the model architecture, these numbers may change. So it is very important to fix the architecture first and then search for the fastest training settings.
