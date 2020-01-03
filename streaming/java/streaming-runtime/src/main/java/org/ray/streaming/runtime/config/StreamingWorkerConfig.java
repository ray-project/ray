package org.ray.streaming.runtime.config;

import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Streaming job worker specified config.
 */
public class StreamingWorkerConfig extends StreamingGlobalConfig {

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamingWorkerConfig.class);

  public StreamingWorkerConfig(Map<String, String> conf) {
    super(conf);

    configMap.putAll(workerConfig2Map());
  }

  public Map<String, String> workerConfig2Map() {
    Map<String, String> result = new HashMap<>();
    try {
    } catch (Exception e) {
      LOGGER.error("Worker config to map occur error.", e);
      return null;
    }
    return result;
  }
}
