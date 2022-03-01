package io.ray.serve.api;

import com.google.common.collect.Lists;
import io.ray.api.ActorHandle;
import io.ray.api.BaseActorHandle;
import io.ray.api.PyActorHandle;
import io.ray.api.Ray;
import io.ray.api.function.PyActorMethod;
import io.ray.serve.DeploymentConfig;
import io.ray.serve.RayServeException;
import io.ray.serve.RayServeHandle;
import io.ray.serve.ReplicaConfig;
import io.ray.serve.ServeController;
import io.ray.serve.generated.EndpointInfo;
import io.ray.serve.util.LogUtil;
import io.ray.serve.util.ServeProtoUtil;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Client {

  private static final Logger LOGGER = LoggerFactory.getLogger(Client.class);

  private BaseActorHandle controller; // TODO change to PyActorHandle

  private boolean shutdown = false;

  private Map<String, RayServeHandle> handleCache = new ConcurrentHashMap<>();

  private String rootUrl;

  private String checkpointPath;

  public Client(BaseActorHandle controller, String controllerName, boolean detached) {
    this.controller = controller;
    this.rootUrl =
        (String)
            ((PyActorHandle) controller)
                .task(PyActorMethod.of("get_root_url"))
                .remote()
                .get(); // TODO use PyActorHandle directly.
    // TODO self._checkpoint_path = ray.get(controller.get_checkpoint_path.remote())
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

  public void deploy(
      String name,
      String deploymentDef,
      Object[] initArgs,
      Map<String, Object> rayActorOptions,
      DeploymentConfig deploymentConfig,
      String version,
      String prevVersion,
      String routePrefix,
      String url,
      Boolean blocking) {

    if (deploymentConfig == null) {
      deploymentConfig = new DeploymentConfig();
    }
    if (rayActorOptions == null) {
      rayActorOptions = new HashMap<>();
    }
    // TODO set runtime_env to rayActorOptions. Not support now.
    ReplicaConfig replicaConfig = new ReplicaConfig(deploymentDef, initArgs, rayActorOptions);
    // TODO ReplicaConfig's PB

    boolean updating =
        (boolean)
            ((PyActorHandle) controller)
                .task(
                    PyActorMethod.of("deploy"),
                    Lists.newArrayList(
                        name,
                        deploymentConfig.toProtobuf(),
                        replicaConfig,
                        version,
                        prevVersion,
                        routePrefix,
                        Ray.getRuntimeContext().getCurrentJobId()))
                .remote()
                .get(); // TODO python ray.task max param length is 5

    String tag = "component=serve deployment=" + name;
    if (updating) {
      String msg = LogUtil.format("Updating deployment '{}'", name);
      if (StringUtils.isNotBlank(version)) {
        msg += LogUtil.format(" to version '{}'", version);
      }
      LOGGER.info("{}. {}", msg, tag);
    } else {
      LOGGER.info(
          "Deployment '{}' is already at version '{}', not updating. {}", name, version, tag);
    }

    if (blocking) {
      waitForDeploymentHealthy(name);
    } else {
      String urlPart = "";
      if (StringUtils.isNotBlank(url)) {
        urlPart = LogUtil.format(" at `{}`", url);
      }
      LOGGER.info(
          "Deployment '{}{}' is ready{}. {}",
          name,
          StringUtils.isNotBlank(version) ? "':'" + version : "",
          urlPart,
          tag);
    }
  }

  /**
   * Waits for the named deployment to enter "HEALTHY" status.
   *
   * <p>Raises RuntimeError if the deployment enters the "UNHEALTHY" status instead.
   *
   * <p>Raises TimeoutError if this doesn't happen before timeout_s.
   *
   * @param name
   */
  private void waitForDeploymentHealthy(String name) {
    // TODO
  }

  /**
   * Completely shut down the connected Serve instance.
   * <p>Shuts down all processes and deletes all state associated with the instance.
   */
  public void shutdown() {
    if (Ray.isInitialized() && !shutdown) {
      ((PyActorHandle) controller)
          .task(PyActorMethod.of("shutdown"))
          .remote()
          .get(); // TODO it has no return value?
      waitForDeploymentsShutdown(null, -1);

      // TODO
    }
  }

  private void waitForDeploymentsShutdown(String name, int timeoutS) {
    // TODO
  }

  public String getRootUrl() {
    return rootUrl;
  }

  public String getCheckpointPath() {
    return checkpointPath;
  }

  public static void main(String[] args) {
    String name = "a";
    String version = "b";
    String urlPart = "c";
    String tag = "d";
    LOGGER.info(
        "Deployment '{}{':'{}}' is ready{}. {}",
        name,
        StringUtils.isNotBlank(version) ? version : null,
        urlPart,
        tag);
  }
}
