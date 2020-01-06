package org.ray.streaming.runtime.config;

import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Streaming job master config.
 */
public class StreamingMasterConfig extends StreamingGlobalConfig {

  private static final Logger LOG = LoggerFactory.getLogger(StreamingMasterConfig.class);

  public StreamingMasterConfig(final Map<String, String> conf) {
    super(conf);
  }
}
