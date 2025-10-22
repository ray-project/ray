.. _train-jit-checkpointing:

Just-In-Time (JIT) Checkpointing
=================================

Overview
--------

Just-In-Time (JIT) checkpointing is a feature that automatically saves a checkpoint when a training worker receives a SIGTERM signal. This minimizes data loss when training jobs are preempted or gracefully terminated.

**When to use JIT checkpointing:**

* Training on preemptible instances (AWS Spot, GCP Preemptible VMs)
* Kubernetes environments with pod preemption
* Jobs managed by schedulers that send SIGTERM before termination (Kueue, YARN)
* Long-running training jobs with infrequent scheduled checkpoints

**Benefits:**

* **Minimal data loss:** Checkpoint is saved when termination is imminent
* **Zero steady-state overhead:** No periodic checkpointing during normal training
* **Simple configuration:** Single boolean flag to enable
* **Works with existing checkpoints:** Complements regular checkpoint schedules

How It Works
------------

JIT checkpointing uses a signal-based approach:

1. **Signal Reception:** When a worker receives SIGTERM, the handler is triggered
2. **Kill Wait Period:** Waits a configurable period (default: 3 seconds) before starting checkpoint
   
   * This optimization avoids wasting time if SIGKILL follows immediately
   * Common in some cluster managers that send SIGTERM then SIGKILL

3. **Async Checkpoint:** Saves checkpoint in a background thread (non-blocking)
4. **Graceful Shutdown:** Worker continues until checkpoint completes

Configuration
-------------

Enable JIT checkpointing through the ``RunConfig``:

.. code-block:: python

    from ray.train import RunConfig
    from ray.train._internal.jit_checkpoint_config import JITCheckpointConfig
    
    run_config = RunConfig(
        jit_checkpoint_config=JITCheckpointConfig(
            enabled=True,      # Enable JIT checkpointing
            kill_wait=3.0,     # Seconds to wait after SIGTERM
        )
    )

Complete Example
----------------

.. code-block:: python

    import tempfile
    import torch
    from ray import train
    from ray.train import Checkpoint, RunConfig, ScalingConfig
    from ray.train._internal.jit_checkpoint_config import JITCheckpointConfig
    from ray.train.torch import TorchTrainer
    
    
    def train_func(config):
        model = torch.nn.Linear(10, 1)
        optimizer = torch.optim.SGD(model.parameters(), lr=0.01)
        
        # Load from checkpoint if resuming
        checkpoint = train.get_checkpoint()
        start_epoch = 0
        
        if checkpoint:
            with checkpoint.as_directory() as checkpoint_dir:
                model.load_state_dict(
                    torch.load(f"{checkpoint_dir}/model.pt")
                )
                start_epoch = torch.load(f"{checkpoint_dir}/epoch.pt") + 1
        
        # Training loop
        for epoch in range(start_epoch, config["num_epochs"]):
            # Training code here...
            loss = train_step(model, optimizer)
            
            # Regular checkpoint every 10 epochs
            if epoch % 10 == 0:
                with tempfile.TemporaryDirectory() as tmpdir:
                    torch.save(model.state_dict(), f"{tmpdir}/model.pt")
                    torch.save(epoch, f"{tmpdir}/epoch.pt")
                    
                    checkpoint = Checkpoint.from_directory(tmpdir)
                    train.report({"epoch": epoch, "loss": loss}, 
                                checkpoint=checkpoint)
            else:
                train.report({"epoch": epoch, "loss": loss})
    
    
    # Configure trainer with JIT checkpointing
    trainer = TorchTrainer(
        train_func,
        train_loop_config={"num_epochs": 100},
        scaling_config=ScalingConfig(num_workers=4),
        run_config=RunConfig(
            jit_checkpoint_config=JITCheckpointConfig(enabled=True),
        ),
    )
    
    result = trainer.fit()

Parameters
----------

``JITCheckpointConfig``
^^^^^^^^^^^^^^^^^^^^^^^

.. py:class:: JITCheckpointConfig

   Configuration for Just-In-Time checkpointing.
   
   :param enabled: Whether to enable JIT checkpointing. Defaults to ``False``.
   :type enabled: bool
   
   :param kill_wait: Seconds to wait after SIGTERM before starting checkpoint.
                     This avoids wasting time if SIGKILL follows immediately.
                     Defaults to 3.0 seconds.
   :type kill_wait: float

Limitations
-----------

JIT checkpointing has several important limitations:

**Signal Coverage**

* Only handles SIGTERM (graceful termination signals)
* Does NOT handle SIGKILL (immediate kill, no time to checkpoint)
* Does NOT handle SIGINT (Ctrl+C) by default

**Failure Scenarios**

* Node failures: If the entire node fails, JIT checkpoint is lost
  
  * Use regular scheduled checkpoints for node failure resilience
  * JIT checkpointing is for worker process failures, not node failures

* Network failures: Cannot save to remote storage if network is down
* OOM errors: Process may be killed before checkpoint completes

**Performance**

* Checkpoint saves to disk (not in-memory)
* Async execution minimizes blocking, but checkpoint still takes time
* Very large models may not checkpoint before SIGKILL arrives

Best Practices
--------------

1. **Combine with Regular Checkpoints**
   
   Use JIT checkpointing alongside periodic checkpoints:
   
   * Periodic checkpoints: Every N epochs (for node failures)
   * JIT checkpoints: On SIGTERM (for preemption)

2. **Configure Kill Wait Appropriately**
   
   * Cloud providers: 3-5 seconds (default 3.0 is good)
   * Kubernetes: 5-30 seconds (depends on terminationGracePeriodSeconds)
   * Local development: 1-2 seconds

3. **Monitor Checkpoint Completion**
   
   Check logs to verify JIT checkpoints complete:
   
   .. code-block:: text
   
      INFO ray.train._internal.jit_checkpoint: SIGTERM received, initiating JIT checkpoint in 3.0s
      INFO ray.train._internal.jit_checkpoint: JIT checkpoint completed in 2.5s

4. **Test Preemption Locally**
   
   Simulate preemption by sending SIGTERM to worker processes:
   
   .. code-block:: bash
   
      # Find Ray worker process
      ps aux | grep ray::RayTrainWorker
      
      # Send SIGTERM
      kill -TERM <process_id>

FAQ
---

**Q: Can I use JIT checkpointing without regular checkpoints?**

A: Not recommended. JIT checkpointing only protects against SIGTERM-based preemption. Use regular checkpoints for other failure modes (node failures, OOM, etc.).

**Q: What happens if checkpoint doesn't complete before SIGKILL?**

A: The checkpoint is lost. Adjust ``kill_wait`` or reduce model size if this happens frequently.

**Q: Does JIT checkpointing work with distributed training?**

A: Yes. Each worker independently handles SIGTERM. Ray Train's existing distributed checkpointing logic merges checkpoint shards.

**Q: Can I customize what gets checkpointed?**

A: Not in the POC. JIT checkpointing uses Ray Train's existing checkpoint infrastructure. Customize by modifying your regular checkpoint saving logic.

**Q: What cloud providers support SIGTERM before termination?**

A: Most major providers:

* AWS: Spot instances receive 2-minute warning with SIGTERM
* GCP: Preemptible VMs receive 30-second warning with SIGTERM
* Azure: Spot VMs receive notice with SIGTERM
* Kubernetes: Pods receive SIGTERM during graceful shutdown

See Also
--------

* :ref:`train-checkpointing`: General checkpointing guide
* :ref:`train-fault-tolerance`: Fault tolerance and recovery
* :ref:`persistent-storage-guide`: Storage configuration

References
----------

This implementation is based on:

* Microsoft Research: "Just-In-Time Checkpointing: Low Cost Error Recovery from Deep Learning Training Failures"
  
  https://www.microsoft.com/en-us/research/publication/just-in-time-checkpointing-low-cost-error-recovery-from-deep-learning-training-failures/

* HuggingFace Transformers JIT Checkpointing Implementation
  
  https://github.com/huggingface/transformers (feat-jit-checkpointing branch)

