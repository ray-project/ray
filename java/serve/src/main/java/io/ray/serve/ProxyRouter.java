package io.ray.serve;

import io.ray.serve.api.Serve;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Default common router for proxy to match incomming routes. */
public class ProxyRouter {

  private static final Logger LOGGER = LoggerFactory.getLogger(ProxyRouter.class);

  /** Key: route, value: endpoint. */
  private Map<String, EndpointInfo> routeInfo = new HashMap<>();

  /** Key: endpointName, value: handle. */
  private Map<String, RayServeHandle> handles = new ConcurrentHashMap<>();

  public void updateRoutes(Map<String, EndpointInfo> endpoints) {
    LOGGER.debug("Got updated endpoints: {}.", endpoints);

    Set<String> existingHandles = new HashSet<>(handles.keySet());
    Map<String, EndpointInfo> routeInfo = new HashMap<>();

    if (endpoints != null) {
      for (Map.Entry<String, EndpointInfo> entry : endpoints.entrySet()) {
        String route =
            StringUtils.isNotBlank(entry.getValue().getRoute())
                ? entry.getValue().getRoute()
                : entry.getKey();
        routeInfo.put(route, entry.getValue());

        if (handles.containsKey(entry.getKey())) {
          existingHandles.remove(entry.getKey());
        } else {
          handles.put(entry.getKey(), Serve.getGlobalClient().getHandle(entry.getKey(), true));
        }
      }
    }

    this.routeInfo = routeInfo;
    for (String endpoint : existingHandles) {
      handles.remove(endpoint);
    }
  }

  /**
   * Return the longest prefix match among existing routes for the route.
   *
   * @param route route to match against.
   * @return serve_handle (RayServeHandle) if found, else null.
   */
  public RayServeHandle matchRoute(String route) {
    EndpointInfo endpointInfo = routeInfo.get(route);
    return endpointInfo == null ? null : handles.get(endpointInfo.getEndpointTag());
  }

  public Map<String, EndpointInfo> getRouteInfo() {
    return routeInfo;
  }

  public Map<String, RayServeHandle> getHandles() {
    return handles;
  }
}
