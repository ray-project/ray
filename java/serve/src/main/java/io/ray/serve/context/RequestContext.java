package io.ray.serve.context;

import java.util.Optional;

public class RequestContext {

  private static RequestContext DEFAULT_CONTEXT = new RequestContext("", "", "", "");

  private static ThreadLocal<RequestContext> SERVE_REQUEST_CONTEXT =
      ThreadLocal.withInitial(() -> DEFAULT_CONTEXT);

  private String route;
  private String requestId;
  private String appName;
  private String multiplexedModelId;

  private RequestContext(
      String route, String requestId, String appName, String multiplexedModelId) {
    this.route = route;
    this.requestId = requestId;
    this.appName = appName;
    this.multiplexedModelId = multiplexedModelId;
  }

  public String getRoute() {
    return route;
  }

  public String getRequestId() {
    return requestId;
  }

  public String getAppName() {
    return appName;
  }

  public String getMultiplexedModelId() {
    return multiplexedModelId;
  }

  public static void set(
      String route, String requestId, String appName, String multiplexedModelId) {
    SERVE_REQUEST_CONTEXT.set(
        new RequestContext(
            Optional.ofNullable(route).orElse(DEFAULT_CONTEXT.getRoute()),
            Optional.ofNullable(requestId).orElse(DEFAULT_CONTEXT.getRequestId()),
            Optional.ofNullable(appName).orElse(DEFAULT_CONTEXT.getAppName()),
            Optional.ofNullable(multiplexedModelId)
                .orElse(DEFAULT_CONTEXT.getMultiplexedModelId())));
  }

  public static RequestContext get() {
    return SERVE_REQUEST_CONTEXT.get();
  }

  public static void clean() {
    SERVE_REQUEST_CONTEXT.remove();
  }
}
