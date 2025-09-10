.. _train-examples-xgboost-external-memory:

Train on Large Datasets with XGBoost External Memory
====================================================

This example demonstrates how to use Ray Train's XGBoost trainer with external memory to train on datasets larger than available RAM.

**Key Features:**

- **One-Step Enablement**: Just add ``use_external_memory=True`` to enable external memory
- **Automatic Conversion**: Ray Train automatically converts datasets to use XGBoost's external memory API
- **Memory Efficiency**: Reduce memory usage by 80-90% compared to standard training
- **Transparent Integration**: No changes needed to existing training code
- **Performance Optimized**: Automatic batch sizing and caching for optimal performance

**What You'll Learn:**

1. How to enable external memory training with a single parameter
2. Memory usage comparison between standard and external memory training
3. Using the convenience method for automatic DMatrix creation
4. Best practices for external memory training
5. Troubleshooting common external memory issues

**Example Code:**

.. testcode::
    :skipif: True

    from ray.train.xgboost import XGBoostTrainer
    from ray.train import ScalingConfig

    # Enable external memory with one parameter
    trainer = XGBoostTrainer(
        train_loop_per_worker=train_fn_per_worker,
        datasets={"train": train_ds, "validation": val_ds},
        scaling_config=ScalingConfig(num_workers=2),
        # 🎯 Just one parameter to enable external memory!
        use_external_memory=True,
        external_memory_cache_dir="/tmp/xgboost_cache",
        external_memory_device="cpu",
        external_memory_batch_size=5000,
    )

    result = trainer.fit()

**Requirements:**

- Ray >= 2.43
- XGBoost >= 1.7.0
- Sufficient disk space for caching (2-3x dataset size)

**When to Use External Memory:**

✅ **Use External Memory When:**
- Dataset size > 50% of available RAM
- Training on large datasets (100GB+)
- Running in memory-constrained environments
- Need to scale training without hardware upgrades

✅ **Stick with Standard Training When:**
- Dataset fits comfortably in memory
- Training on small to medium datasets
- Maximum performance is critical
- Simple setup is preferred

**Benefits:**

- **Scale to Any Size**: Train on datasets larger than available RAM
- **Memory Efficient**: Reduce memory usage by 80-90%
- **Automatic Optimization**: Ray Train handles batch sizing and caching
- **Transparent Integration**: No changes needed to existing code
- **Production Ready**: Built on XGBoost's official external memory API

**Complete Example:**

.. literalinclude:: examples/xgboost_external_memory.py
    :language: python
    :start-after: """
    :end-before: if __name__ == "__main__":

**Next Steps:**

1. Run the example to see external memory in action
2. Try with your own large datasets
3. Customize cache directory and batch size for your hardware
4. Monitor performance and optimize as needed

For more information, see the :ref:`XGBoost External Memory Guide <train-xgboost-external-memory>`.
