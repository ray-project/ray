package org.ray.runtime.config;

public enum RunMode {
  SINGLE_PROCESS(true, false), // dev path, dev runtime
  SINGLE_BOX(true, true),      // dev path, native runtime
  CLUSTER(false, true);        // deploy path, naive runtime


  RunMode(boolean devPathManager,
      boolean nativeRuntime) {
    this.devPathManager = devPathManager;
    this.nativeRuntime = nativeRuntime;
  }

  /**
   * the jar has add to java -cp, no need to load jar after started.
   */
  private final boolean devPathManager;

  private final boolean nativeRuntime;

  /**
   * Getter method for property <tt>devPathManager</tt>.
   *
   * @return property value of devPathManager
   */
  public boolean isDevPathManager() {
    return devPathManager;
  }

  /**
   * Getter method for property <tt>nativeRuntime</tt>.
   *
   * @return property value of nativeRuntime
   */
  public boolean isNativeRuntime() {
    return nativeRuntime;
  }
}