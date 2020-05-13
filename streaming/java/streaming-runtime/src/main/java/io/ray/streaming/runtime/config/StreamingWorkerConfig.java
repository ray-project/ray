package io.ray.streaming.runtime.config;

import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Streaming job worker specified config.
 */
public class StreamingWorkerConfig extends StreamingGlobalConfig {

  private static final Logger LOG = LoggerFactory.getLogger(StreamingWorkerConfig.class);

  public StreamingWorkerConfig(final Map<String, String> conf) {
    super(conf);
  }
}
