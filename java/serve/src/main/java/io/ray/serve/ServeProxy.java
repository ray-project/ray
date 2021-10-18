package io.ray.serve;

import java.util.Map;

public interface ServeProxy {

  void init(Map<String, String> config, ProxyRouter proxyRouter);

  default String getName() {
    return getClass().getName();
  }

  default void registerServiceDiscovery() {}
}
