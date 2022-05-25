package io.ray.serve.proxy;

import java.util.Map;

import io.ray.serve.router.ProxyRouter;

public interface ServeProxy {

  void init(Map<String, String> config, ProxyRouter proxyRouter);

  default String getName() {
    return getClass().getName();
  }

  default void registerServiceDiscovery() {}
}
