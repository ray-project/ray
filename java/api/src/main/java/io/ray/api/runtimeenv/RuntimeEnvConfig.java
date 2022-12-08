package io.ray.api.runtimeenv;

/** A POJO class used to specify configuration options for a runtime environment. */
public class RuntimeEnvConfig {
  /**
   * The timeout of runtime environment creation, timeout is in seconds. The value `-1` means
   * disable timeout logic, except `-1`, `setup_timeout_seconds` cannot be less than or equal to 0.
   * The default value of `setup_timeout_seconds` is 600 seconds.
   */
  private Integer setupTimeoutSeconds = 600;
  /**
   * Indicates whether to install the runtime environment on the cluster at `ray.init()` time,
   * before the workers are leased. This flag is set to `True` by default.
   */
  private Boolean eagerInstall = true;

  public RuntimeEnvConfig() {}

  public RuntimeEnvConfig(int setupTimeoutSeconds, boolean eagerInstall) {
    this.setupTimeoutSeconds = setupTimeoutSeconds;
    this.eagerInstall = eagerInstall;
  }

  public void setSetupTimeoutSeconds(int setupTimeoutSeconds) {
    this.setupTimeoutSeconds = setupTimeoutSeconds;
  }

  public Integer getSetupTimeoutSeconds() {
    return this.setupTimeoutSeconds;
  }

  public void setEagerInstall(boolean eagerInstall) {
    this.eagerInstall = eagerInstall;
  }

  public Boolean getEagerInstall() {
    return eagerInstall;
  }
}
