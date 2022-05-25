package io.ray.serve.api;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import io.ray.api.ActorHandle;
import io.ray.api.BaseActorHandle;
import io.ray.api.PyActorHandle;
import io.ray.api.Ray;
import io.ray.api.function.PyActorMethod;
import io.ray.serve.ServeController;
import io.ray.serve.exception.RayServeException;
import io.ray.serve.generated.DeploymentStatus;
import io.ray.serve.generated.DeploymentStatusInfoList;
import io.ray.serve.generated.EndpointInfo;
import io.ray.serve.handle.RayServeHandle;
import io.ray.serve.model.DeploymentConfig;
import io.ray.serve.model.DeploymentInfo;
import io.ray.serve.model.DeploymentStatusInfo;
import io.ray.serve.model.ReplicaConfig;
import io.ray.serve.util.LogUtil;
import io.ray.serve.util.ServeProtoUtil;

public class ServeControllerClient {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServeControllerClient.class);

  private static long CLIENT_POLLING_INTERVAL_S = 1;

  private BaseActorHandle controller; // TODO change to PyActorHandle

  private String controllerName;

  private boolean detached;

  private String overrideControllerNamespace;

  private boolean shutdown;

  private Map<String, RayServeHandle> handleCache = new ConcurrentHashMap<>();

  private String rootUrl;

  private String checkpointPath;

  public ServeControllerClient(
      BaseActorHandle controller,
      String controllerName,
      boolean detached,
      String overrideControllerNamespace) {
    this.controller = controller;
    this.controllerName = controllerName;
    this.detached = detached;
    this.overrideControllerNamespace = overrideControllerNamespace;
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
    // TODO set runtime_env to rayActorOptions is not supported now.
    ReplicaConfig replicaConfig = new ReplicaConfig(deploymentDef, initArgs, rayActorOptions);

    if (deploymentConfig.getAutoscalingConfig() != null
        && deploymentConfig.getMaxConcurrentQueries()
            < deploymentConfig.getAutoscalingConfig().getTargetNumOngoingRequestsPerReplica()) {
      LOGGER.warn(
          "Autoscaling will never happen, because 'max_concurrent_queries' is less than 'target_num_ongoing_requests_per_replica' now.");
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
                    Ray.getRuntimeContext().getCurrentJobId())
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
      waitForDeploymentHealthy(name, -1);
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
   */
  private void waitForDeploymentHealthy(String name, long timeoutS) {
    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < timeoutS * 1000 || timeoutS < 0) {
      Map<String, DeploymentStatusInfo> statuses = getDeploymentStatuses();
      DeploymentStatusInfo status = statuses.get(name);
      if (status == null) {
        throw new RayServeException(
            LogUtil.format(
                "Waiting for deployment {} to be HEALTHY, but deployment doesn't exist.", name));
      }

      if (status.getDeploymentStatus() == DeploymentStatus.HEALTHY) {
        break;
      } else if (status.getDeploymentStatus() == DeploymentStatus.UNHEALTHY) {
        throw new RayServeException(
            LogUtil.format("Deployment {} is UNHEALTHY: {}", name, status.getMessage()));
      } else {
        Preconditions.checkState(status.getDeploymentStatus() == DeploymentStatus.UPDATING);
      }

      LOGGER.debug(
          "Waiting for {} to be healthy, current status: {}.", name, status.getDeploymentStatus());
      try {
        Thread.sleep(CLIENT_POLLING_INTERVAL_S * 1000);
      } catch (InterruptedException e) {
      }
    }
    throw new RayServeException(
        LogUtil.format("Deployment {} did not become HEALTHY after {}s.", name, timeoutS));
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
        String controllerNamespace =
            Serve.getControllerNamespace(detached, overrideControllerNamespace);
        Optional<BaseActorHandle> controllerHandle =
            Ray.getActor(controllerName, controllerNamespace);
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
  @SuppressWarnings("unchecked")
  private void waitForDeploymentsShutdown(long timeoutS) {
    long start = System.currentTimeMillis();
    List<String> statuses = null;
    while (System.currentTimeMillis() - start < timeoutS * 1000) {
      statuses =
          (List<String>)
              ((PyActorHandle) controller)
                  .task(PyActorMethod.of("get_deployment_statuses"))
                  .remote()
                  .get(); // TODO define PB of Dict[str, DeploymentStatusInfo]

      if (statuses == null || statuses.size() == 0) {
        return;
      } else {
        LOGGER.debug("Waiting for shutdown, {} deployments still alive.", statuses.size());
      }
    }
    throw new RayServeException(
        LogUtil.format(
            "Shutdown didn't complete after {}s. Deployments still alive: {}.",
            timeoutS,
            statuses));
  }

  public String getRootUrl() { // TODO
    return rootUrl;
  }

  public String getCheckpointPath() {
    return checkpointPath;
  }

  public DeploymentInfo getDeploymentInfo(String name) {
    return (DeploymentInfo)
        ((PyActorHandle) controller)
            .task(PyActorMethod.of("get_deployment_info"), name)
            .remote()
            .get(); // TODO define protobuf of DeploymentInfo
  }

  @SuppressWarnings("unchecked")
  public Map<String, DeploymentInfo> listDeployments() {
    return (Map<String, DeploymentInfo>)
        ((PyActorHandle) controller)
            .task(PyActorMethod.of("list_deployments"))
            .remote()
            .get(); // TODO define protobuf of DeploymentInfo
  }

  public void deleteDeployment(String name) {
    ((PyActorHandle) controller).task(PyActorMethod.of("delete_deployment")).remote();
    waitForDeploymentDeleted(name, 60);
  }

  /**
   * Waits for the named deployment to be shut down and deleted.
   *
   * <p>Raises TimeoutError if this doesn't happen before timeoutS. // TODO
   *
   * @param name
   * @param timeoutS
   */
  @SuppressWarnings("unchecked")
  private void waitForDeploymentDeleted(String name, long timeoutS) {
    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < timeoutS * 1000) {
      List<String> statuses =
          (List<String>)
              ((PyActorHandle) controller)
                  .task(PyActorMethod.of("get_deployment_statuses"))
                  .remote()
                  .get(); // TODO define PB of Dict[str, DeploymentStatusInfo]

      if (!statuses.contains(name)) {
        break;
      }
      LOGGER.debug("Waiting for {} to be deleted, current status: {}.", name, statuses);
      try {
        Thread.sleep(CLIENT_POLLING_INTERVAL_S * 1000);
      } catch (InterruptedException e) {
      }
    }
    throw new RayServeException(
        LogUtil.format("Deployment {} wasn't deleted after {}s.", name, timeoutS));
  }

  private Map<String, DeploymentStatusInfo> getDeploymentStatuses() {
    byte[] deploymentStatusInfoListProtoBytes =
        (byte[])
            ((PyActorHandle) controller)
                .task(PyActorMethod.of("get_deployment_statuses"))
                .remote()
                .get();
    DeploymentStatusInfoList deploymentStatusInfoList =
        ServeProtoUtil.bytesToProto(
            deploymentStatusInfoListProtoBytes, bytes -> DeploymentStatusInfoList.parseFrom(bytes));

    Map<String, DeploymentStatusInfo> deploymentStatuses = new HashMap<>();
    for (io.ray.serve.generated.DeploymentStatusInfo deploymentStatusInfoProto :
        deploymentStatusInfoList.getDeploymentStatusInfosList()) {
      deploymentStatuses.put(
          deploymentStatusInfoProto.getName(),
          DeploymentStatusInfo.fromProto(deploymentStatusInfoProto));
    }
    return deploymentStatuses;
  }
}
