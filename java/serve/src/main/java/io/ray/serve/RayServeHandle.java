package io.ray.serve;

import com.google.common.collect.ImmutableMap;
import io.ray.api.BaseActorHandle;
import io.ray.api.ObjectRef;
import io.ray.runtime.metric.Count;
import io.ray.runtime.metric.Metrics;
import io.ray.serve.generated.RequestMetadata;
import org.apache.commons.lang3.RandomStringUtils;

public class RayServeHandle {

  private String endpointName;

  private HandleOptions handleOptions;

  private String handleTag;

  private Count requestCounter;

  private Router router;

  public RayServeHandle(
      BaseActorHandle controllerHandle,
      String endpointName,
      HandleOptions handleOptions,
      Router router) {
    this.endpointName = endpointName;
    this.handleOptions = handleOptions != null ? handleOptions : new HandleOptions();
    this.handleTag = endpointName + "#" + RandomStringUtils.randomAlphabetic(6);
    this.router = router != null ? router : new Router(controllerHandle, endpointName);
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
                            endpointName))
                    .register());
  }

  /**
   * Returns a Ray ObjectRef whose results can be waited for or retrieved using ray.wait or ray.get
   * (or ``await object_ref``), respectively.
   *
   * @param parameters The input parameters of the specified method to be invoked in the deployment.
   * @return ray.ObjectRef
   */
  public ObjectRef<Object> remote(Object[] parameters) {
    RayServeMetrics.execute(() -> requestCounter.inc(1.0));
    RequestMetadata.Builder requestMetadata = RequestMetadata.newBuilder();
    requestMetadata.setRequestId(RandomStringUtils.randomAlphabetic(10));
    requestMetadata.setEndpoint(endpointName);
    requestMetadata.setCallMethod(
        handleOptions != null ? handleOptions.getMethodName() : Constants.CALL_METHOD);
    return router.assignRequest(requestMetadata.build(), parameters);
  }

  public RayServeHandle setMethodName(String methodName) {
    handleOptions.setMethodName(methodName);
    return this;
  }

  public Router getRouter() {
    return router;
  }
}
