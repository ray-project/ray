package org.ray.streaming.runtime.config.global;

import org.ray.streaming.runtime.config.Config;

/**
 * Job common config.
 */
public interface CommonConfig extends Config {

  String JOB_ID = "streaming.job.id";
  String JOB_NAME = "streaming.job.name";
  String FILE_ENCODING = "streaming.file.encoding";

  @DefaultValue(value = "default-job-id")
  @Key(value = JOB_ID)
  String jobId();

  @DefaultValue(value = "default-job-name")
  @Key(value = JOB_NAME)
  String jobName();

  @DefaultValue(value = "UTF-8")
  @Key(value = FILE_ENCODING)
  String fileEncoding();
}
