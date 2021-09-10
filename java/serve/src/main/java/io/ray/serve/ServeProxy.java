package io.ray.serve;

import java.util.Map;

public interface ServeProxy {

  void init(Map<String, String> config, Router router);

  default String getName() {
    return getClass().getName();
  }

  default void updateRoutes(Map<String, EndpointInfo> endpoints) {

  }

  default void registerServiceDiscovery() {

  }
}
