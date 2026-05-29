package io.ray.serve.handle;

import io.ray.runtime.metric.Count;
import io.ray.runtime.metric.Metrics;
import io.ray.serve.api.Serve;
import io.ray.serve.common.Constants;
import io.ray.serve.context.RequestContext;
import io.ray.serve.deployment.DeploymentId;
import io.ray.serve.generated.RequestMetadata;
import io.ray.serve.metrics.RayServeMetrics;
import io.ray.serve.router.Router;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;

/** A handle to a service deployment. */
public class DeploymentHandle implements Serializable {

  private static final long serialVersionUID = 550701184372753496L;

  private DeploymentId deploymentId;

  private HandleOptions handleOptions;

  private String handleTag;

  private transient Count requestCounter;

  private transient volatile Router router;

  public DeploymentHandle(String deploymentName, String appName) {
    this(deploymentName, appName, null, null);
  }

  public DeploymentHandle(
      String deploymentName, String appName, HandleOptions handleOptions, Router router) {
    this.deploymentId = new DeploymentId(deploymentName, appName);
    this.handleOptions = handleOptions != null ? handleOptions : new HandleOptions();
    this.handleTag =
        StringUtils.isBlank(appName)
            ? deploymentName + Constants.SEPARATOR_HASH + RandomStringUtils.randomAlphabetic(6)
            : appName
                + Constants.SEPARATOR_HASH
                + deploymentName
                + Constants.SEPARATOR_HASH
                + RandomStringUtils.randomAlphabetic(6);
    this.router = router;
    initMetrics();
  }

  private void initMetrics() {
    Map<String, String> metricsTags = new HashMap<>();
    metricsTags.put(RayServeMetrics.TAG_HANDLE, handleTag);
    metricsTags.put(RayServeMetrics.TAG_ENDPOINT, deploymentId.getName());
    if (StringUtils.isNotBlank(deploymentId.getApp())) {
      metricsTags.put(RayServeMetrics.TAG_APPLICATION, deploymentId.getApp());
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
  public DeploymentResponse remote(Object... parameters) {
    RequestContext requestContext = RequestContext.get();
    RayServeMetrics.execute(() -> requestCounter.inc(1.0));
    RequestMetadata.Builder requestMetadata = RequestMetadata.newBuilder();
    requestMetadata.setRequestId(requestContext.getRequestId());
    requestMetadata.setCallMethod(
        handleOptions != null ? handleOptions.getMethodName() : Constants.CALL_METHOD);
    requestMetadata.setRoute(requestContext.getRoute());
    requestMetadata.setMultiplexedModelId(requestContext.getMultiplexedModelId());
    return new DeploymentResponse(
        getOrCreateRouter().assignRequest(requestMetadata.build(), parameters));
  }

  private Router getOrCreateRouter() {
    if (router != null) {
      return router;
    }

    synchronized (DeploymentHandle.class) {
      if (router != null) {
        return router;
      }
      router = new Router(Serve.getGlobalClient().getController(), deploymentId);
    }
    return router;
  }

  /**
   * The constructor of DeploymentHandle is not invoked during deserialization, so it is necessary
   * to customize some initialization behavior during the deserialization process.
   *
   * @param in
   * @throws IOException
   * @throws ClassNotFoundException
   */
  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    initMetrics();
  }

  public DeploymentHandle method(String methodName) {
    handleOptions.setMethodName(methodName);
    return this;
  }

  // TODO method(String methodName, String signature)

  public Router getRouter() {
    return getOrCreateRouter();
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
