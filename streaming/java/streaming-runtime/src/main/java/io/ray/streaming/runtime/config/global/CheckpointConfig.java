package io.ray.streaming.runtime.config.global;

import io.ray.streaming.runtime.config.Config;
import org.aeonbits.owner.Mutable;

public interface CheckpointConfig extends Config, Mutable {

  String CP_INTERVAL_SECS = "streaming.checkpoint.interval.secs";
  String CP_TIMEOUT_SECS = "streaming.checkpoint.timeout.secs";
  String CP_PARTIAL_TIMEOUT_SECS = "streaming.checkpoint.partial.timeout.secs";
  String CP_MODE = "streaming.checkpoint.mode";
  String CP_RETAINED_NUM = "streaming.checkpoint.retained.num";

  String CP_PREFIX_KEY_MASTER = "streaming.checkpoint.prefix-key.job-master.context";
  String CP_PREFIX_KEY_WORKER = "streaming.checkpoint.prefix-key.job-worker.context";
  String CP_PREFIX_KEY_OPERATOR = "streaming.checkpoint.prefix-key.job-worker.operator";

  @DefaultValue(value = "5")
  @Key(value = CP_INTERVAL_SECS)
  int cpIntervalSecs();

  @DefaultValue(value = "120")
  @Key(value = CP_TIMEOUT_SECS)
  int cpTimeoutSecs();

  @DefaultValue(value = "120")
  @Key(value = CP_PARTIAL_TIMEOUT_SECS)
  int cpPartialTimeoutSecs();

  @DefaultValue(value = "sync")
  @Key(value = CP_MODE)
  String cpMode();

  @DefaultValue(value = "1")
  @Key(value = CP_RETAINED_NUM)
  int cpRetainedNum();

  @DefaultValue(value = "job_master_runtime_context_")
  @Key(value = CP_PREFIX_KEY_MASTER)
  String jobMasterContextCpPrefixKey();

  @DefaultValue(value = "job_worker_context_")
  @Key(value = CP_PREFIX_KEY_WORKER)
  String jobWorkerContextCpPrefixKey();

  @DefaultValue(value = "job_worker_op_")
  @Key(value = CP_PREFIX_KEY_OPERATOR)
  String jobWorkerOpCpPrefixKey();
}
