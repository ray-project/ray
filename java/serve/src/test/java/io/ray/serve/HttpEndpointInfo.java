package io.ray.serve;

import java.util.List;
import java.util.Map;

public class HttpEndpointInfo {

  private String endpointTag;

  private List<String> httpMethods; // TODO change to set.

  private String route;

  private Map<String, String> config;

  public String getEndpointTag() {
    return endpointTag;
  }

  public void setEndpointTag(String endpointTag) {
    this.endpointTag = endpointTag;
  }

  public List<String> getHttpMethods() {
    return httpMethods;
  }

  public void setHttpMethods(List<String> httpMethods) {
    this.httpMethods = httpMethods;
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
