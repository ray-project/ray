package io.ray.serve;

import io.ray.serve.generated.EndpointInfo;
import java.util.Map;

public interface ServeProxy {

  void init(Map<String, String> config, ProxyRouter router);

  default String getName() {
    return getClass().getName();
  }

  default void updateRoutes(Map<String, EndpointInfo> endpoints) {}

  default void registerServiceDiscovery() {}
}
