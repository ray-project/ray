package io.ray.serve.context;

public class RequestContext {
    String route;
    String requestId;
    String appName;
    String multiplexedModelId;

    public RequestContext(String route, String requestId, String appName, String multiplexedModelId) {
      this.route = route;
      this.requestId = requestId;
      this.appName = appName;
      this.multiplexedModelId = multiplexedModelId;
    }

  public String getRoute() {
      return route;
    }

    public void setRoute(String route) {
      this.route = route;
    }

    public String getRequestId() {
      return requestId;
    }

    public void setRequestId(String requestId) {
      this.requestId = requestId;
    }

    public String getAppName() {
      return appName;
    }

    public void setAppName(String appName) {
      this.appName = appName;
    }

    public String getMultiplexedModelId() {
      return multiplexedModelId;
    }

    public void setMultiplexedModelId(String multiplexedModelId) {
      this.multiplexedModelId = multiplexedModelId;
    }
}
