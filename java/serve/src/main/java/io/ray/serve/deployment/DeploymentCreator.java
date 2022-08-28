package io.ray.serve.deployment;

import com.google.common.base.Preconditions;
import io.ray.serve.config.AutoscalingConfig;
import io.ray.serve.config.DeploymentConfig;
import io.ray.serve.generated.DeploymentLanguage;
import java.util.Map;

public class DeploymentCreator {

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
  private String version;

  /**
   * Version of the existing deployment which is used as a precondition for the next deployment. If
   * prev_version does not match with the existing deployment's version, the deployment will fail.
   * If not provided, deployment procedure will not check the existing deployment's version.
   */
  private String prevVersion;

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

  /**
   * Requests to paths under this HTTP path prefix will be routed to this deployment. Defaults to
   * '/{name}'. When set to 'None', no HTTP endpoint will be created. Routing is done based on
   * longest-prefix match, so if you have deployment A with a prefix of '/a' and deployment B with a
   * prefix of '/a/b', requests to '/a', '/a/', and '/a/c' go to A and requests to '/a/b', '/a/b/',
   * and '/a/b/c' go to B. Routes must not end with a '/' unless they're the root (just '/'), which
   * acts as a catch-all.
   */
  private String routePrefix;

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
  private Integer maxConcurrentQueries;

  private AutoscalingConfig autoscalingConfig;

  private Double gracefulShutdownWaitLoopS;

  private Double gracefulShutdownTimeoutS;

  private Double healthCheckPeriodS;

  private Double healthCheckTimeoutS;

  private boolean routed;

  private DeploymentLanguage language;

  public Deployment create() {

    Preconditions.checkArgument(
        numReplicas == null || numReplicas == 0 || autoscalingConfig == null,
        "Manually setting num_replicas is not allowed when autoscalingConfig is provided.");

    DeploymentConfig config =
        new DeploymentConfig()
            .setNumReplicas(numReplicas)
            .setMaxConcurrentQueries(maxConcurrentQueries)
            .setUserConfig(userConfig)
            .setAutoscalingConfig(autoscalingConfig)
            .setGracefulShutdownWaitLoopS(gracefulShutdownWaitLoopS)
            .setGracefulShutdownTimeoutS(gracefulShutdownTimeoutS)
            .setHealthCheckPeriodS(healthCheckPeriodS)
            .setHealthCheckTimeoutS(healthCheckTimeoutS)
            .setDeploymentLanguage(language);

    return new Deployment(
        deploymentDef,
        name,
        config,
        version,
        prevVersion,
        initArgs,
        routed ? routePrefix : "/" + name,
        rayActorOptions);
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

  public String getPrevVersion() {
    return prevVersion;
  }

  public DeploymentCreator setPrevVersion(String prevVersion) {
    this.prevVersion = prevVersion;
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

  public String getRoutePrefix() {
    return routePrefix;
  }

  public DeploymentCreator setRoutePrefix(String routePrefix) {
    this.routePrefix = routePrefix;
    this.routed = true;
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

  public Integer getMaxConcurrentQueries() {
    return maxConcurrentQueries;
  }

  public DeploymentCreator setMaxConcurrentQueries(Integer maxConcurrentQueries) {
    this.maxConcurrentQueries = maxConcurrentQueries;
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
