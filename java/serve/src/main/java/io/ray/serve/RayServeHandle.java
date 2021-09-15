package io.ray.serve;

import com.google.common.collect.ImmutableMap;
import io.ray.api.BaseActorHandle;
import io.ray.api.ObjectRef;
import io.ray.runtime.metric.Count;
import io.ray.runtime.metric.Metrics;
import org.apache.commons.lang3.RandomStringUtils;

public class RayServeHandle {

  private String endpointName;

  private String handleTag;

  private Count requestCounter;

  private Router router;

  public RayServeHandle(BaseActorHandle controllerHandle, String endpointName) {
    this.endpointName = endpointName;
    this.handleTag = endpointName + "#" + RandomStringUtils.randomAlphabetic(6);
    this.router = new Router(controllerHandle, endpointName);
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
   * @param parameters input parameters of specified method.
   * @param handleOptions specified options
   * @return ray.ObjectRef
   */
  public ObjectRef<Object> remote(Object[] parameters, HandleOptions handleOptions) {

    RayServeMetrics.execute(() -> requestCounter.inc(1.0));
    io.ray.serve.generated.RequestMetadata.Builder requestMetadata =
        io.ray.serve.generated.RequestMetadata.newBuilder(); // TODO merge the pre PR.

    requestMetadata.setRequestId(RandomStringUtils.randomAlphabetic(10));
    requestMetadata.setEndpoint(endpointName);
    requestMetadata.setCallMethod(
        handleOptions != null ? handleOptions.getMethodName() : null); // TODO default call.

    return router.assignRequest(requestMetadata.build(), parameters);
  }
}
