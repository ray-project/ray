package io.ray.serve;

import com.google.common.collect.ImmutableMap;
import io.ray.api.BaseActorHandle;
import io.ray.api.Ray;
import io.ray.runtime.metric.Count;
import io.ray.runtime.metric.MetricConfig;
import io.ray.runtime.metric.Metrics;
import org.apache.commons.lang3.RandomStringUtils;

public class RayServeHandle {

  private BaseActorHandle controllerHandle;

  private String endpointName;

  private String handleTag;

  private Count requestCounter;

  public RayServeHandle(BaseActorHandle controllerHandle, String endpointName) {
    this.controllerHandle = controllerHandle;
    this.endpointName = endpointName;
    this.handleTag = endpointName + "#" + RandomStringUtils.randomAlphabetic(6);
    registerMetrics();
  }

  public Object remote(Object[] parameters) {
    return null;
  }

  private void registerMetrics() {
    if (!Ray.isInitialized() || Ray.getRuntimeContext().isSingleProcess()) {
      return;
    }

    Metrics.init(MetricConfig.DEFAULT_CONFIG);
    requestCounter =
        Metrics.count()
            .name("serve_handle_request_counter")
            .description("The number of handle.remote() calls that have been made on this handle.")
            .unit("")
            .tags(ImmutableMap.of("handle", handleTag, "endpoint", endpointName))
            .register();
  }
}
