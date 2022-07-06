package io.ray.serve.proxy;

import java.util.Map;

public interface ServeProxy {

  void init(Map<String, String> config, ProxyRouter proxyRouter);

  default String getName() {
    return getClass().getName();
  }

  default void registerServiceDiscovery() {}
}
