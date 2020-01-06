package org.ray.streaming.runtime.config.global;

import org.ray.streaming.runtime.config.Config;

/**
 * Job common config.
 */
public interface CommonConfig extends Config {

  String JOB_ID = "streaming.job.id";
  String JOB_NAME = "streaming.job.name";
  String FILE_ENCODING = "streaming.file.encoding";

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

  /**
   * Default encoding type. e.g. UTF-8, GBK
   * @return Encoding type with string type.
   */
  @DefaultValue(value = "UTF-8")
  @Key(value = FILE_ENCODING)
  String fileEncoding();
}
