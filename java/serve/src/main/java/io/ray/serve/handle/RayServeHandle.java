package io.ray.serve.handle;

import com.google.common.collect.ImmutableMap;
import io.ray.api.BaseActorHandle;
import io.ray.api.ObjectRef;
import io.ray.runtime.metric.Count;
import io.ray.runtime.metric.Metrics;
import io.ray.serve.common.Constants;
import io.ray.serve.generated.RequestMetadata;
import io.ray.serve.metrics.RayServeMetrics;
import io.ray.serve.router.Router;
import org.apache.commons.lang3.RandomStringUtils;

/** A handle to a service deployment. */
public class RayServeHandle {

  private String deploymentName;

  private HandleOptions handleOptions;

  private String handleTag;

  private Count requestCounter;

  private Router router;

  public RayServeHandle(
      BaseActorHandle controllerHandle,
      String deploymentName,
      HandleOptions handleOptions,
      Router router) {
    this.deploymentName = deploymentName;
    this.handleOptions = handleOptions != null ? handleOptions : new HandleOptions();
    this.handleTag = deploymentName + "#" + RandomStringUtils.randomAlphabetic(6);
    this.router = router != null ? router : new Router(controllerHandle, deploymentName);
    RayServeMetrics.execute(
        () ->
            this.requestCounter =
                Metrics.count()
                    .name(RayServeMetrics.SERVE_HANDLE_REQUEST_COUNTER.name())
                    .description(RayServeMetrics.SERVE_HANDLE_REQUEST_COUNTER.getDescription())
                    .unit("")
                    .tags(
                        ImmutableMap.of(
                            RayServeMetrics.TAG_HANDLE,
                            handleTag,
                            RayServeMetrics.TAG_ENDPOINT,
                            deploymentName))
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
    requestMetadata.setEndpoint(deploymentName);
    requestMetadata.setCallMethod(
        handleOptions != null ? handleOptions.getMethodName() : Constants.CALL_METHOD);
    return router.assignRequest(requestMetadata.build(), parameters);
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
