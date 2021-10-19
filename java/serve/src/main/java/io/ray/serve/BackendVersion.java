package io.ray.serve;

import java.io.Serializable;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;

public class BackendVersion implements Serializable {

  private static final long serialVersionUID = 3400261981775851058L;

  private String codeVersion;

  private Object userConfig;

  private boolean unversioned;

  public BackendVersion() {
    this(null, null);
  }

  public BackendVersion(String codeVersion) {
    this(codeVersion, null);
  }

  public BackendVersion(String codeVersion, Object userConfig) {
    if (StringUtils.isBlank(codeVersion)) {
      this.unversioned = true;
      this.codeVersion = RandomStringUtils.randomAlphabetic(6);
    } else {
      this.codeVersion = codeVersion;
    }
    this.userConfig = userConfig;
  }

  public String getCodeVersion() {
    return codeVersion;
  }

  public Object getUserConfig() {
    return userConfig;
  }

  public boolean isUnversioned() {
    return unversioned;
  }
}
