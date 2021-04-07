package io.ray.streaming.runtime.config;

import java.io.Serializable;
import java.util.Map;

/** Streaming config including general, master and worker part. */
public class StreamingConfig implements Serializable {

  public StreamingMasterConfig masterConfig;
  public StreamingWorkerConfig workerConfigTemplate;

  public StreamingConfig(final Map<String, String> conf) {
    masterConfig = new StreamingMasterConfig(conf);
    workerConfigTemplate = new StreamingWorkerConfig(conf);
  }

  public Map<String, String> getMap() {
    Map<String, String> wholeConfigMap = masterConfig.configMap;
    wholeConfigMap.putAll(workerConfigTemplate.configMap);
    return wholeConfigMap;
  }
}
