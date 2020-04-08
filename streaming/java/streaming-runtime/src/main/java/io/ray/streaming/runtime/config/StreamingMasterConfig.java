package io.ray.streaming.runtime.config;

import io.ray.streaming.runtime.config.master.ResourceConfig;
import java.util.Map;
import org.aeonbits.owner.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Streaming job master config.
 */
public class StreamingMasterConfig extends StreamingGlobalConfig {

  private static final Logger LOG = LoggerFactory.getLogger(StreamingMasterConfig.class);

  public ResourceConfig resourceConfig;

  public StreamingMasterConfig(final Map<String, String> conf) {
    super(conf);
    this.resourceConfig = ConfigFactory.create(ResourceConfig.class, conf);
  }
}
