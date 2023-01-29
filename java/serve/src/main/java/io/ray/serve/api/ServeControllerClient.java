package io.ray.serve.api;

import com.google.common.base.Preconditions;
import io.ray.api.ActorHandle;
import io.ray.api.BaseActorHandle;
import io.ray.api.PyActorHandle;
import io.ray.api.Ray;
import io.ray.api.function.PyActorMethod;
import io.ray.serve.common.Constants;
import io.ray.serve.config.DeploymentConfig;
import io.ray.serve.config.ReplicaConfig;
import io.ray.serve.controller.ServeController;
import io.ray.serve.deployment.DeploymentRoute;
import io.ray.serve.exception.RayServeException;
import io.ray.serve.generated.DeploymentRouteList;
import io.ray.serve.generated.DeploymentStatus;
import io.ray.serve.generated.DeploymentStatusInfo;
import io.ray.serve.generated.EndpointInfo;
import io.ray.serve.generated.StatusOverview;
import io.ray.serve.handle.RayServeHandle;
import io.ray.serve.util.LogUtil;
import io.ray.serve.util.ServeProtoUtil;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServeControllerClient {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServeControllerClient.class);

  private static long CLIENT_POLLING_INTERVAL_S = 1;

  private BaseActorHandle controller; // TODO change to PyActorHandle

  private String controllerName;

  private boolean detached; // TODO if non-detached, shutdown serve when process exits.

  private boolean shutdown;

  private Map<String, RayServeHandle> handleCache = new ConcurrentHashMap<>();

  private String rootUrl;

  @SuppressWarnings("unchecked")
  public ServeControllerClient(
      BaseActorHandle controller, String controllerName, boolean detached) {
    this.controller = controller;
    this.controllerName = controllerName;
    this.detached = detached;
    this.rootUrl =
        controller instanceof PyActorHandle
            ? (String)
                ((PyActorHandle) controller).task(PyActorMethod.of("get_root_url")).remote().get()
            : ((ActorHandle<ServeController>) controller)
                .task(ServeController::getRootUrl)
                .remote()
                .get();
  }

  /**
   * Retrieve RayServeHandle for service deployment to invoke it from Java.
   *
   * @param deploymentName A registered service deployment.
   * @param missingOk If true, then Serve won't check the deployment is registered.
   * @return
   */
  @SuppressWarnings("unchecked")
  public RayServeHandle getHandle(String deploymentName, boolean missingOk) {
    String cacheKey = deploymentName + "#" + missingOk;
    if (handleCache.containsKey(cacheKey)) {
      return handleCache.get(cacheKey);
    }

    Map<String, EndpointInfo> endpoints = null;
    if (controller instanceof PyActorHandle) {
      endpoints =
          ServeProtoUtil.parseEndpointSet(
              (byte[])
                  ((PyActorHandle) controller)
                      .task(PyActorMethod.of(Constants.CONTROLLER_GET_ALL_ENDPOINTS_METHOD))
                      .remote()
                      .get());
    } else {
      LOGGER.warn("Client currently only supports the Python controller.");
      endpoints =
          ServeProtoUtil.parseEndpointSet(
              ((ActorHandle<? extends ServeController>) controller)
                  .task(ServeController::getAllEndpoints)
                  .remote()
                  .get());
    }

    if (!missingOk && (endpoints == null || !endpoints.containsKey(deploymentName))) {
      throw new RayServeException(LogUtil.format("Deployment {} does not exist.", deploymentName));
    }

    RayServeHandle handle = new RayServeHandle(controller, deploymentName, null, null);
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
    // TODO set runtime_env to rayActorOptions is not supported now.
    ReplicaConfig replicaConfig = new ReplicaConfig(deploymentDef, initArgs, rayActorOptions);

    deploymentConfig.setVersion(version);
    deploymentConfig.setPrevVersion(prevVersion);

    if (deploymentConfig.getAutoscalingConfig() != null
        && deploymentConfig.getMaxConcurrentQueries()
            < deploymentConfig.getAutoscalingConfig().getTargetNumOngoingRequestsPerReplica()) {
      LOGGER.warn(
          "Autoscaling will never happen, because 'max_concurrent_queries' is less than 'target_num_ongoing_requests_per_replica'.");
    }

    boolean updating =
        (boolean)
            ((PyActorHandle) controller)
                .task(
                    PyActorMethod.of("deploy"),
                    name,
                    deploymentConfig.toProtoBytes(),
                    replicaConfig.toProtoBytes(),
                    routePrefix,
                    Ray.getRuntimeContext().getCurrentJobId().getBytes())
                .remote()
                .get();

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
      String urlPart = url != null ? LogUtil.format(" at `{}`", url) : "";
      LOGGER.info(
          "Deployment '{}{}' is ready {}. {}",
          name,
          StringUtils.isNotBlank(version) ? "':'" + version : "",
          urlPart,
          tag);
    }
  }

  /**
   * Waits for the named deployment to enter "HEALTHY" status.
   *
   * <p>Raises RayServeException if the deployment enters the "UNHEALTHY" status instead or this
   * doesn't happen before timeoutS.
   *
   * @param name
   * @param timeoutS
   */
  private void waitForDeploymentHealthy(String name, Long timeoutS) {
    long start = System.currentTimeMillis();
    boolean isTimeout = true;
    while (timeoutS == null || System.currentTimeMillis() - start < timeoutS * 1000) {
      DeploymentStatusInfo status = getDeploymentStatus(name);
      if (status == null) {
        throw new RayServeException(
            LogUtil.format(
                "Waiting for deployment {} to be HEALTHY, but deployment doesn't exist.", name));
      }

      if (status.getStatus() == DeploymentStatus.HEALTHY) {
        isTimeout = false;
        break;
      } else if (status.getStatus() == DeploymentStatus.UNHEALTHY) {
        throw new RayServeException(
            LogUtil.format("Deployment {} is UNHEALTHY: {}", name, status.getMessage()));
      } else {
        Preconditions.checkState(status.getStatus() == DeploymentStatus.UPDATING);
      }

      LOGGER.debug("Waiting for {} to be healthy, current status: {}.", name, status.getStatus());
      try {
        Thread.sleep(CLIENT_POLLING_INTERVAL_S * 1000);
      } catch (InterruptedException e) {
      }
    }
    if (isTimeout) {
      throw new RayServeException(
          LogUtil.format("Deployment {} did not become HEALTHY after {}s.", name, timeoutS));
    }
  }

  private void waitForDeploymentHealthy(String name) {
    waitForDeploymentHealthy(name, null);
  }

  /**
   * Completely shut down the connected Serve instance.
   *
   * <p>Shuts down all processes and deletes all state associated with the instance.
   */
  public synchronized void shutdown() {
    if (Ray.isInitialized() && !shutdown) {
      ((PyActorHandle) controller).task(PyActorMethod.of("shutdown")).remote();
      waitForDeploymentsShutdown(60);

      controller.kill();

      long started = System.currentTimeMillis();
      while (true) {
        Optional<BaseActorHandle> controllerHandle =
            Ray.getActor(controllerName, Constants.SERVE_NAMESPACE);
        if (!controllerHandle.isPresent()) {
          // actor name is removed
          break;
        }
        long currentTime = System.currentTimeMillis();
        if (currentTime - started > 5000) {
          LOGGER.warn(
              "Waited 5s for Serve to shutdown gracefully but the controller is still not cleaned up. You can ignore this warning if you are shutting down the Ray cluster.");
          break;
        }
      }

      shutdown = true;
    }
  }

  /**
   * Waits for all deployments to be shut down and deleted.
   *
   * <p>Throw RayServeException if this doesn't happen before timeoutS.
   *
   * @param timeoutS
   */
  private void waitForDeploymentsShutdown(long timeoutS) {
    long start = System.currentTimeMillis();
    List<DeploymentStatusInfo> deploymentStatuses = null;
    while (System.currentTimeMillis() - start < timeoutS * 1000) {
      StatusOverview statusOverview = getServeStatus();
      if (statusOverview == null
          || statusOverview.getDeploymentStatuses() == null
          || statusOverview.getDeploymentStatuses().getDeploymentStatusInfosList() == null
          || statusOverview.getDeploymentStatuses().getDeploymentStatusInfosList().isEmpty()) {
        return;
      }
      deploymentStatuses = statusOverview.getDeploymentStatuses().getDeploymentStatusInfosList();
      LOGGER.debug("Waiting for shutdown, {} deployments still alive.", deploymentStatuses.size());
      try {
        Thread.sleep(CLIENT_POLLING_INTERVAL_S * 1000);
      } catch (InterruptedException e) {
      }
    }
    List<String> liveNames = new ArrayList<>();
    if (deploymentStatuses != null) {
      for (DeploymentStatusInfo status : deploymentStatuses) {
        liveNames.add(status.getName());
      }
    }

    throw new RayServeException(
        LogUtil.format(
            "Shutdown didn't complete after {}s. Deployments still alive: {}.",
            timeoutS,
            liveNames));
  }

  public String getRootUrl() {
    return rootUrl;
  }

  public DeploymentRoute getDeploymentInfo(String name) {
    return DeploymentRoute.fromProtoBytes(
        (byte[])
            ((PyActorHandle) controller)
                .task(PyActorMethod.of("get_deployment_info"), name)
                .remote()
                .get());
  }

  public Map<String, DeploymentRoute> listDeployments() {
    DeploymentRouteList deploymentRouteList =
        ServeProtoUtil.bytesToProto(
            (byte[])
                ((PyActorHandle) controller)
                    .task(PyActorMethod.of("list_deployments"))
                    .remote()
                    .get(),
            DeploymentRouteList::parseFrom);

    if (deploymentRouteList == null || deploymentRouteList.getDeploymentRoutesList() == null) {
      return Collections.emptyMap();
    }

    Map<String, DeploymentRoute> deploymentRoutes =
        new HashMap<>(deploymentRouteList.getDeploymentRoutesList().size());
    for (io.ray.serve.generated.DeploymentRoute deploymentRoute :
        deploymentRouteList.getDeploymentRoutesList()) {
      deploymentRoutes.put(
          deploymentRoute.getDeploymentInfo().getName(),
          DeploymentRoute.fromProto(deploymentRoute));
    }
    return deploymentRoutes;
  }

  public void deleteDeployment(String name, boolean blocking) { // TODO update to deleteDeployments
    ((PyActorHandle) controller).task(PyActorMethod.of("delete_deployment")).remote();
    if (blocking) {
      waitForDeploymentDeleted(name, 60);
    }
  }

  /**
   * Waits for the named deployment to be shut down and deleted.
   *
   * <p>Throw RayServeException if this doesn't happen before timeoutS. // TODO change to
   * TimeoutException
   *
   * @param name
   * @param timeoutS
   */
  private void waitForDeploymentDeleted(String name, long timeoutS) {
    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < timeoutS * 1000) {
      DeploymentStatusInfo status = getDeploymentStatus(name);
      if (status == null) {
        break;
      }
      LOGGER.debug("Waiting for {} to be deleted, current status: {}.", name, status);
      try {
        Thread.sleep(CLIENT_POLLING_INTERVAL_S * 1000);
      } catch (InterruptedException e) {
      }
    }
    throw new RayServeException(
        LogUtil.format("Deployment {} wasn't deleted after {}s.", name, timeoutS));
  }

  private StatusOverview getServeStatus() {
    return ServeProtoUtil.bytesToProto(
        (byte[])
            ((PyActorHandle) controller).task(PyActorMethod.of("get_serve_status")).remote().get(),
        StatusOverview::parseFrom);
  }

  private DeploymentStatusInfo getDeploymentStatus(String name) {
    return ServeProtoUtil.bytesToProto(
        (byte[])
            ((PyActorHandle) controller)
                .task(PyActorMethod.of("get_deployment_status"), name)
                .remote()
                .get(),
        DeploymentStatusInfo::parseFrom);
  }

  public BaseActorHandle getController() {
    return controller;
  }
}
