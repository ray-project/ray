package io.ray.streaming.runtime.config;

import io.ray.streaming.runtime.config.worker.WorkerInternalConfig;
import java.util.HashMap;
import java.util.Map;
import org.aeonbits.owner.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Streaming job worker specified config.
 */
public class StreamingWorkerConfig extends StreamingGlobalConfig {

  private static final Logger LOG = LoggerFactory.getLogger(StreamingWorkerConfig.class);

  public WorkerInternalConfig workerInternalConfig;

  public StreamingWorkerConfig(final Map<String, String> conf) {
    super(conf);
    workerInternalConfig = ConfigFactory.create(WorkerInternalConfig.class, conf);

    configMap.putAll(workerConfig2Map());
  }

  public Map<String, String> workerConfig2Map() {
    Map<String, String> result = new HashMap<>();
    try {
      result.putAll(config2Map(this.workerInternalConfig));
    } catch (Exception e) {
      LOG.error("Worker config to map occur error.", e);
      return null;
    }
    return result;
  }

}
