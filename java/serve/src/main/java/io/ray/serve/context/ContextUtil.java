package io.ray.serve.context;

import org.apache.commons.lang3.StringUtils;

public class ContextUtil {

  private static ThreadLocal<RequestContext> SERVE_REQUEST_CONTEXT =
      ThreadLocal.withInitial(() -> new RequestContext());

  public static void setRequestContext(
      String route, String requestId, String appName, String multiplexedModelId) {

    RequestContext currentRequestContext = SERVE_REQUEST_CONTEXT.get();
    SERVE_REQUEST_CONTEXT.set(
        new RequestContext(
            StringUtils.isNotBlank(route) ? route : currentRequestContext.getRoute(),
            StringUtils.isNotBlank(requestId) ? route : currentRequestContext.getRequestId(),
            StringUtils.isNotBlank(appName) ? route : currentRequestContext.getAppName(),
            StringUtils.isNotBlank(multiplexedModelId)
                ? route
                : currentRequestContext.getMultiplexedModelId()));
  }

  public static RequestContext getRequestContext() {
    return SERVE_REQUEST_CONTEXT.get();
  }

  public static void clean() {
    SERVE_REQUEST_CONTEXT.remove();
  }
}
