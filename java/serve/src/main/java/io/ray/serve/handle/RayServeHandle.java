package io.ray.serve.handle;

import io.ray.api.ObjectRef;
import io.ray.runtime.metric.Count;
import io.ray.runtime.metric.Metrics;
import io.ray.serve.api.Serve;
import io.ray.serve.common.Constants;
import io.ray.serve.deployment.DeploymentId;
import io.ray.serve.generated.RequestMetadata;
import io.ray.serve.metrics.RayServeMetrics;
import io.ray.serve.router.Router;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;

/** A handle to a service deployment. */
public class RayServeHandle {

  private DeploymentId deploymentId;

  private HandleOptions handleOptions;

  private String handleTag;

  private Count requestCounter;

  private Router router;

  public RayServeHandle(
      String deploymentName, String appName, HandleOptions handleOptions, Router router) {
    this.deploymentId = new DeploymentId(deploymentName, appName);
    this.handleOptions = handleOptions != null ? handleOptions : new HandleOptions();
    this.handleTag =
        StringUtils.isBlank(appName)
            ? deploymentName + "#" + RandomStringUtils.randomAlphabetic(6)
            : appName + "#" + deploymentName + "#" + RandomStringUtils.randomAlphabetic(6);
    this.router = router;

    Map<String, String> metricsTags = new HashMap<>();
    metricsTags.put(RayServeMetrics.TAG_HANDLE, handleTag);
    metricsTags.put(RayServeMetrics.TAG_ENDPOINT, deploymentName);
    if (StringUtils.isNotBlank(appName)) {
      metricsTags.put(RayServeMetrics.TAG_APPLICATION, appName);
    }
    RayServeMetrics.execute(
        () ->
            this.requestCounter =
                Metrics.count()
                    .name(RayServeMetrics.SERVE_HANDLE_REQUEST_COUNTER.name())
                    .description(RayServeMetrics.SERVE_HANDLE_REQUEST_COUNTER.getDescription())
                    .unit("")
                    .tags(metricsTags)
                    .register());
  }

  /**
   * Returns a Ray ObjectRef whose results can be waited for or retrieved using ray.wait or Ray.get
   * (or ``await object_ref``), respectively.
   *
   * @param parameters The input parameters of the specified method to be invoked in the deployment.
   * @return ObjectRef
   */
  public ObjectRef<Object> remote(Object... parameters) {
    RayServeMetrics.execute(() -> requestCounter.inc(1.0));
    RequestMetadata.Builder requestMetadata = RequestMetadata.newBuilder();
    requestMetadata.setRequestId(RandomStringUtils.randomAlphabetic(10));
    requestMetadata.setEndpoint(deploymentId.getName());
    requestMetadata.setCallMethod(
        handleOptions != null ? handleOptions.getMethodName() : Constants.CALL_METHOD);
    return getOrCreateRouter().assignRequest(requestMetadata.build(), parameters);
  }

  private Router getOrCreateRouter() {
    if (router != null) {
      return router;
    }

    synchronized (RayServeHandle.class) {
      if (router != null) {
        return router;
      }
      router = new Router(Serve.getGlobalClient().getController(), deploymentId.getName());
    }
    return router;
  }

  public RayServeHandle method(String methodName) {
    handleOptions.setMethodName(methodName);
    return this;
  }

  // TODO method(String methodName, String signature)

  public Router getRouter() {
    return router;
  }

  /**
   * Whether this handle is actively polling for replica updates.
   *
   * @return
   */
  public boolean isPolling() {
    return router.getLongPollClient().isRunning();
  }
}
