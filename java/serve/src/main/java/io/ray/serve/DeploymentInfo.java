package io.ray.serve;

import java.io.Serializable;
import java.util.Map;

public class DeploymentInfo implements Serializable {

  private static final long serialVersionUID = -7132135316463505391L;

  private String name;

  private String backendDef;

  private Object[] initArgs;

  private BackendConfig backendConfig;

  private BackendVersion backendVersion;

  private Map<String, String> config;

  public String getName() {
    return name;
  }

  public DeploymentInfo setName(String name) {
    this.name = name;
    return this;
  }

  public String getBackendDef() {
    return backendDef;
  }

  public DeploymentInfo setBackendDef(String backendDef) {
    this.backendDef = backendDef;
    return this;
  }

  public Object[] getInitArgs() {
    return initArgs;
  }

  public DeploymentInfo setInitArgs(Object[] initArgs) {
    this.initArgs = initArgs;
    return this;
  }

  public BackendConfig getBackendConfig() {
    return backendConfig;
  }

  public DeploymentInfo setBackendConfig(BackendConfig backendConfig) {
    this.backendConfig = backendConfig;
    return this;
  }

  public BackendVersion getBackendVersion() {
    return backendVersion;
  }

  public DeploymentInfo setBackendVersion(BackendVersion backendVersion) {
    this.backendVersion = backendVersion;
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
