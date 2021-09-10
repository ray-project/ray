package io.ray.serve;

import java.util.Map;

public class EndpointInfo {

  private String endpointTag;

  private String route;

  private Map<String, String> config;

  public String getEndpointTag() {
    return endpointTag;
  }

  public void setEndpointTag(String endpointTag) {
    this.endpointTag = endpointTag;
  }

  public String getRoute() {
    return route;
  }

  public void setRoute(String route) {
    this.route = route;
  }

  public Map<String, String> getConfig() {
    return config;
  }

  public void setConfig(Map<String, String> config) {
    this.config = config;
  }
}
