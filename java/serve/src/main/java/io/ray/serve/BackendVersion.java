package io.ray.serve;

import java.io.Serializable;

public class BackendVersion implements Serializable {

  private static final long serialVersionUID = 3400261981775851058L;

  private String codeVersion;

  private Object userConfig;

  public String getCodeVersion() {
    return codeVersion;
  }

  public BackendVersion setCodeVersion(String codeVersion) {
    this.codeVersion = codeVersion;
    return this;
  }

  public Object getUserConfig() {
    return userConfig;
  }

  public BackendVersion setUserConfig(Object userConfig) {
    this.userConfig = userConfig;
    return this;
  }
}
