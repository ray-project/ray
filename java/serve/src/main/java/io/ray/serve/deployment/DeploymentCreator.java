package io.ray.serve.deployment;

import com.google.common.base.Preconditions;
import io.ray.serve.api.Serve;
import io.ray.serve.config.AutoscalingConfig;
import io.ray.serve.config.DeploymentConfig;
import io.ray.serve.config.ReplicaConfig;
import io.ray.serve.generated.DeploymentLanguage;
import io.ray.serve.util.CommonUtil;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeploymentCreator {

  private static final Logger LOGGER = LoggerFactory.getLogger(Serve.class);

  private String deploymentDef;

  /**
   * Globally-unique name identifying this deployment. If not provided, the name of the class or
   * function will be used.
   */
  private String name;

  /**
   * Version of the deployment. This is used to indicate a code change for the deployment; when it
   * is re-deployed with a version change, a rolling update of the replicas will be performed. If
   * not provided, every deployment will be treated as a new version.
   */
  @Deprecated private String version;

  /**
   * The number of processes to start up that will handle requests to this deployment. Defaults to
   * 1.
   */
  private Integer numReplicas;

  /**
   * Positional args to be passed to the class constructor when starting up deployment replicas.
   * These can also be passed when you call `.deploy()` on the returned Deployment.
   */
  private Object[] initArgs;

  /** Options to be passed to the Ray actor constructor such as resource requirements. */
  private Map<String, Object> rayActorOptions;

  /**
   * [experimental] Config to pass to the reconfigure method of the deployment. This can be updated
   * dynamically without changing the version of the deployment and restarting its replicas.
   */
  private Object userConfig;

  /**
   * The maximum number of queries that will be sent to a replica of this deployment without
   * receiving a response. Defaults to 100.
   */
  private Integer maxOngoingRequests;

  private AutoscalingConfig autoscalingConfig;

  private Double gracefulShutdownWaitLoopS;

  private Double gracefulShutdownTimeoutS;

  private Double healthCheckPeriodS;

  private Double healthCheckTimeoutS;

  private DeploymentLanguage language;

  // TODO is_driver_deployment\placement_group_bundles\placement_group_strategy

  public Deployment create(boolean check) {

    if (check) {
      Preconditions.checkArgument(
          numReplicas == null || numReplicas != 0, "num_replicas is expected to larger than 0");

      Preconditions.checkArgument(
          numReplicas == null || autoscalingConfig == null,
          "Manually setting num_replicas is not allowed when autoscalingConfig is provided.");
    }

    if (version != null) {
      LOGGER.warn(
          "DeprecationWarning: `version` in `@serve.deployment` has been deprecated. Explicitly specifying version will raise an error in the future!");
    }

    DeploymentConfig deploymentConfig =
        new DeploymentConfig()
            .setNumReplicas(numReplicas != null ? numReplicas : 1)
            .setMaxOngoingRequests(maxOngoingRequests)
            .setUserConfig(userConfig)
            .setAutoscalingConfig(autoscalingConfig)
            .setGracefulShutdownWaitLoopS(gracefulShutdownWaitLoopS)
            .setGracefulShutdownTimeoutS(gracefulShutdownTimeoutS)
            .setHealthCheckPeriodS(healthCheckPeriodS)
            .setHealthCheckTimeoutS(healthCheckTimeoutS)
            .setDeploymentLanguage(language);

    ReplicaConfig replicaConfig = new ReplicaConfig(deploymentDef, initArgs, rayActorOptions);

    return new Deployment(
        StringUtils.isNotBlank(name) ? name : CommonUtil.getDeploymentName(deploymentDef),
        deploymentConfig,
        replicaConfig,
        version);
  }

  public Deployment create() {
    return create(true);
  }

  public Application bind(Object... args) {
    return create().bind(args);
  }

  public String getDeploymentDef() {
    return deploymentDef;
  }

  public DeploymentCreator setDeploymentDef(String deploymentDef) {
    this.deploymentDef = deploymentDef;
    return this;
  }

  public String getName() {
    return name;
  }

  public DeploymentCreator setName(String name) {
    this.name = name;
    return this;
  }

  public String getVersion() {
    return version;
  }

  public DeploymentCreator setVersion(String version) {
    this.version = version;
    return this;
  }

  public Integer getNumReplicas() {
    return numReplicas;
  }

  public DeploymentCreator setNumReplicas(Integer numReplicas) {
    this.numReplicas = numReplicas;
    return this;
  }

  public Object[] getInitArgs() {
    return initArgs;
  }

  public DeploymentCreator setInitArgs(Object[] initArgs) {
    this.initArgs = initArgs;
    return this;
  }

  public Map<String, Object> getRayActorOptions() {
    return rayActorOptions;
  }

  public DeploymentCreator setRayActorOptions(Map<String, Object> rayActorOptions) {
    this.rayActorOptions = rayActorOptions;
    return this;
  }

  public Object getUserConfig() {
    return userConfig;
  }

  public DeploymentCreator setUserConfig(Object userConfig) {
    this.userConfig = userConfig;
    return this;
  }

  public Integer getMaxOngoingRequests() {
    return maxOngoingRequests;
  }

  public DeploymentCreator setMaxOngoingRequests(Integer maxOngoingRequests) {
    this.maxOngoingRequests = maxOngoingRequests;
    return this;
  }

  public AutoscalingConfig getAutoscalingConfig() {
    return autoscalingConfig;
  }

  public DeploymentCreator setAutoscalingConfig(AutoscalingConfig autoscalingConfig) {
    this.autoscalingConfig = autoscalingConfig;
    return this;
  }

  public Double getGracefulShutdownWaitLoopS() {
    return gracefulShutdownWaitLoopS;
  }

  public DeploymentCreator setGracefulShutdownWaitLoopS(Double gracefulShutdownWaitLoopS) {
    this.gracefulShutdownWaitLoopS = gracefulShutdownWaitLoopS;
    return this;
  }

  public Double getGracefulShutdownTimeoutS() {
    return gracefulShutdownTimeoutS;
  }

  public DeploymentCreator setGracefulShutdownTimeoutS(Double gracefulShutdownTimeoutS) {
    this.gracefulShutdownTimeoutS = gracefulShutdownTimeoutS;
    return this;
  }

  public Double getHealthCheckPeriodS() {
    return healthCheckPeriodS;
  }

  public DeploymentCreator setHealthCheckPeriodS(Double healthCheckPeriodS) {
    this.healthCheckPeriodS = healthCheckPeriodS;
    return this;
  }

  public Double getHealthCheckTimeoutS() {
    return healthCheckTimeoutS;
  }

  public DeploymentCreator setHealthCheckTimeoutS(Double healthCheckTimeoutS) {
    this.healthCheckTimeoutS = healthCheckTimeoutS;
    return this;
  }

  public DeploymentLanguage getLanguage() {
    return language;
  }

  public DeploymentCreator setLanguage(DeploymentLanguage language) {
    this.language = language;
    return this;
  }
}
