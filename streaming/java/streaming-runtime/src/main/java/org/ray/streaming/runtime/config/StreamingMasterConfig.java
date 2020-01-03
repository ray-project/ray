package org.ray.streaming.runtime.config;

import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Streaming job master config.
 */
public class StreamingMasterConfig extends StreamingGlobalConfig {

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamingMasterConfig.class);

  public StreamingMasterConfig(Map<String, String> conf) {
    super(conf);

    // configMap.putAll(masterConfig2Map());
  }

  @Override
  public String toString() {
    return configMap.toString();
  }

  /*
  public Map<String, String> masterConfig2Map() {
    Map<String, String> result = new HashMap<>();

    try {
      result.putAll(config2Map(this.resourceConfig));
    } catch (Exception e) {
      LOGGER.error("Global config to map occur error.", e);
      return null;
    }

    return result;
  }

   */
}
