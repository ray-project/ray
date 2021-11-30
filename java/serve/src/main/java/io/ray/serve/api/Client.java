package io.ray.serve.api;

import io.ray.api.ActorHandle;
import io.ray.api.BaseActorHandle;
import io.ray.api.PyActorHandle;
import io.ray.api.function.PyActorMethod;
import io.ray.serve.RayServeException;
import io.ray.serve.RayServeHandle;
import io.ray.serve.ServeController;
import io.ray.serve.generated.EndpointInfo;
import io.ray.serve.util.LogUtil;
import io.ray.serve.util.ServeProtoUtil;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Client {

  private static final Logger LOGGER = LoggerFactory.getLogger(Client.class);

  private BaseActorHandle controller;

  private Map<String, RayServeHandle> handleCache = new ConcurrentHashMap<>();

  public Client(BaseActorHandle controller, String controllerName, boolean detached) {
    this.controller = controller;
  }

  /**
   * Retrieve RayServeHandle for service endpoint to invoke it from Python.
   *
   * @param endpointName A registered service endpoint.
   * @param missingOk If true, then Serve won't check the endpoint is registered. False by default.
   * @return
   */
  @SuppressWarnings("unchecked")
  public RayServeHandle getHandle(String endpointName, boolean missingOk) {

    String cacheKey = endpointName + "_" + missingOk;
    if (handleCache.containsKey(cacheKey)) {
      return handleCache.get(cacheKey);
    }

    Map<String, EndpointInfo> endpoints = null;
    if (controller instanceof PyActorHandle) {
      endpoints =
          ServeProtoUtil.parseEndpointSet(
              (byte[])
                  ((PyActorHandle) controller)
                      .task(PyActorMethod.of("get_all_endpoints"))
                      .remote()
                      .get());
    } else {
      LOGGER.warn("Client only support Python controller now.");
      endpoints =
          ServeProtoUtil.parseEndpointSet(
              ((ActorHandle<? extends ServeController>) controller)
                  .task(ServeController::getAllEndpoints)
                  .remote()
                  .get());
    }

    if (!missingOk && (endpoints == null || !endpoints.containsKey(endpointName))) {
      throw new RayServeException(LogUtil.format("Endpoint {} does not exist.", endpointName));
    }

    RayServeHandle handle = new RayServeHandle(controller, endpointName, null, null);
    handleCache.put(cacheKey, handle);
    return handle;
  }
}
