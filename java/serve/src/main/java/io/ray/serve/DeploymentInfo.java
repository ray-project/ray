package io.ray.serve;

import java.io.Serializable;
import java.util.Map;

public class DeploymentInfo implements Serializable {

  private static final long serialVersionUID = -7132135316463505391L;

  private String name;

  private String deploymentDef;

  private Object[] initArgs;

  private DeploymentConfig deploymentConfig;

  private DeploymentVersion deploymentVersion;

  private Map<String, String> config;

  public String getName() {
    return name;
  }

  public DeploymentInfo setName(String name) {
    this.name = name;
    return this;
  }

  public String getDeploymentDef() {
    return deploymentDef;
  }

  public DeploymentInfo setDeploymentDef(String deploymentDef) {
    this.deploymentDef = deploymentDef;
    return this;
  }

  public Object[] getInitArgs() {
    return initArgs;
  }

  public DeploymentInfo setInitArgs(Object[] initArgs) {
    this.initArgs = initArgs;
    return this;
  }

  public DeploymentConfig getDeploymentConfig() {
    return deploymentConfig;
  }

  public DeploymentInfo setDeploymentConfig(DeploymentConfig deploymentConfig) {
    this.deploymentConfig = deploymentConfig;
    return this;
  }

  public DeploymentVersion getDeploymentVersion() {
    return deploymentVersion;
  }

  public DeploymentInfo setDeploymentVersion(DeploymentVersion deploymentVersion) {
    this.deploymentVersion = deploymentVersion;
    return this;
  }

  public Map<String, String> getConfig() {
    return config;
  }

  public DeploymentInfo setConfig(Map<String, String> config) {
    this.config = config;
    return this;
  }
}
