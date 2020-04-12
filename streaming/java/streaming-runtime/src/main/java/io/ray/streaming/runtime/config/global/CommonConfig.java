package io.ray.streaming.runtime.config.global;

import io.ray.streaming.runtime.config.Config;

/**
 * Job common config.
 */
public interface CommonConfig extends Config {

  String JOB_ID = "streaming.job.id";
  String JOB_NAME = "streaming.job.name";

  /**
   * Ray streaming job id. Non-custom.
   * @return Job id with string type.
   */
  @DefaultValue(value = "default-job-id")
  @Key(value = JOB_ID)
  String jobId();

  /**
   * Ray streaming job name. Non-custom.
   * @return Job name with string type.
   */
  @DefaultValue(value = "default-job-name")
  @Key(value = JOB_NAME)
  String jobName();
}
