package io.ray.streaming.runtime.config.global;

import io.ray.streaming.runtime.config.Config;
import org.aeonbits.owner.Mutable;

/** Configurations for checkpointing. */
public interface CheckpointConfig extends Config, Mutable {

  String CP_INTERVAL_SECS = "streaming.checkpoint.interval.secs";
  String CP_TIMEOUT_SECS = "streaming.checkpoint.timeout.secs";

  String CP_PREFIX_KEY_MASTER = "streaming.checkpoint.prefix-key.job-master.context";
  String CP_PREFIX_KEY_WORKER = "streaming.checkpoint.prefix-key.job-worker.context";
  String CP_PREFIX_KEY_OPERATOR = "streaming.checkpoint.prefix-key.job-worker.operator";

  /**
   * Checkpoint time interval. JobMaster won't trigger 2 checkpoint in less than this time interval.
   */
  @DefaultValue(value = "5")
  @Key(value = CP_INTERVAL_SECS)
  int cpIntervalSecs();

  /**
   * How long should JobMaster wait for checkpoint to finish. When this timeout is reached and
   * JobMaster hasn't received all commits from workers, JobMaster will consider this checkpoint as
   * failed and trigger another checkpoint.
   */
  @DefaultValue(value = "120")
  @Key(value = CP_TIMEOUT_SECS)
  int cpTimeoutSecs();

  /**
   * This is used for saving JobMaster's context to storage, user usually don't need to change this.
   */
  @DefaultValue(value = "job_master_runtime_context_")
  @Key(value = CP_PREFIX_KEY_MASTER)
  String jobMasterContextCpPrefixKey();

  /**
   * This is used for saving JobWorker's context to storage, user usually don't need to change this.
   */
  @DefaultValue(value = "job_worker_context_")
  @Key(value = CP_PREFIX_KEY_WORKER)
  String jobWorkerContextCpPrefixKey();

  /**
   * This is used for saving user operator(in StreamTask)'s context to storage, user usually don't
   * need to change this.
   */
  @DefaultValue(value = "job_worker_op_")
  @Key(value = CP_PREFIX_KEY_OPERATOR)
  String jobWorkerOpCpPrefixKey();
}
