package io.ray.serve;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.ray.serve.api.Serve;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpRouter {

  private static final Logger LOGGER = LoggerFactory.getLogger(HttpRouter.class);

  private static Gson gson = new Gson();

  /** Key: route, value: endpoint. */
  private Map<String, HttpEndpointInfo> routeInfo = new HashMap<>();

  /** Key: endpointName, value: handle. */
  private Map<String, RayServeHandle> handles = new ConcurrentHashMap<>();

  public void updateRoutes(Map<String, EndpointInfo> endpoints) {
    LOGGER.debug("Got updated endpoints: {}.", endpoints);

    Set<String> existingHandles = new HashSet<>(handles.keySet());
    Map<String, HttpEndpointInfo> routeInfo = new HashMap<>();

    if (endpoints != null) {
      endpoints.forEach(
          (key, value) -> {
            HttpEndpointInfo httpEndpointInfo = parseEndpointInfo(value);
            routeInfo.put(
                StringUtils.isNotBlank(httpEndpointInfo.getRoute())
                    ? httpEndpointInfo.getRoute()
                    : key,
                httpEndpointInfo);

            if (handles.containsKey(key)) {
              existingHandles.remove(key);
            } else {
              handles.put(key, Serve.getGlobalClient().getHandle(key, true));
            }
          });
    }

    this.routeInfo = routeInfo;
    for (String endpoint : existingHandles) {
      handles.remove(endpoint);
    }
  }

  private HttpEndpointInfo parseEndpointInfo(EndpointInfo endpointInfo) {
    if (endpointInfo == null) {
      return null;
    }

    HttpEndpointInfo httpEndpointInfo = new HttpEndpointInfo();
    httpEndpointInfo.setEndpointTag(endpointInfo.getEndpointTag());
    httpEndpointInfo.setRoute(endpointInfo.getRoute());
    httpEndpointInfo.setConfig(endpointInfo.getConfig());

    Optional.ofNullable(endpointInfo.getConfig())
        .map(config -> config.get("ray.serve.proxy.http.methods"))
        .map(methods -> gson.fromJson(methods, new TypeToken<List<String>>() {}.getType()))
        .ifPresent(httpMethods -> httpEndpointInfo.setHttpMethods((List<String>) httpMethods));

    return httpEndpointInfo;
  }

  public RayServeHandle matchRoute(String route, String method) {
    HttpEndpointInfo httpEndpointInfo = routeInfo.get(route);
    if (httpEndpointInfo == null) {
      return null;
    }

    if (httpEndpointInfo.getHttpMethods() == null
        || !httpEndpointInfo.getHttpMethods().contains(method)) {
      return null;
    }

    return handles.get(httpEndpointInfo.getEndpointTag());
  }

  public Map<String, HttpEndpointInfo> getRouteInfo() {
    return routeInfo;
  }
}
