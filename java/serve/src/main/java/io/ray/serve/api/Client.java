package io.ray.serve.api;

import com.google.gson.Gson;
import io.ray.api.BaseActorHandle;
import io.ray.api.PyActorHandle;
import io.ray.api.function.PyActorMethod;
import io.ray.serve.EndpointInfo;
import io.ray.serve.RayServeException;
import io.ray.serve.RayServeHandle;
import io.ray.serve.util.LogUtil;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Client {

  private static final Gson GSON = new Gson();

  private BaseActorHandle controller;

  private String controllerName;

  private boolean detached;

  private boolean shutdown = false;

  private Map<String, String> config;

  private Map<String, RayServeHandle> handleCache = new ConcurrentHashMap<>();

  public Client(BaseActorHandle controller, String controllerName, boolean detached) {
    this.controller = controller;
    this.controllerName = controllerName;
    this.detached = detached;

    String configs =
        (String) ((PyActorHandle) controller).task(PyActorMethod.of("get_config")).remote().get();
    this.config = GSON.fromJson(configs, Map.class); // TODO
  }

  /**
   * Retrieve RayServeHandle for service endpoint to invoke it from Python.
   *
   * @param endpointName A registered service endpoint.
   * @param missingOk If true, then Serve won't check the endpoint is registered. False by default.
   * @return
   */
  public RayServeHandle getHandle(String endpointName, boolean missingOk) {

    String cacheKey = endpointName + "_" + missingOk;
    if (handleCache.containsKey(cacheKey)) {
      return handleCache.get(cacheKey);
    }

    String endpointJson =
        (String)
            ((PyActorHandle) controller).task(PyActorMethod.of("get_all_endpoints")).remote().get();
    Map<String, EndpointInfo> endpoints = GSON.fromJson(endpointJson, Map.class); // TODO

    if (!missingOk && !endpoints.containsKey(endpointName)) {
      throw new RayServeException(LogUtil.format("Endpoint {} does not exist.", endpointName));
    }

    RayServeHandle handle = new RayServeHandle(controller, endpointName);
    handleCache.put(cacheKey, handle);
    return handle;
  }
}
