package org.ray.core.model;

public enum RunMode {
  SINGLE_PROCESS(true, false, true, false), // remote lambda, dev path, dev runtime
  SINGLE_BOX(true, false, true, true),      // remote lambda, dev path, native runtime
  CLUSTER(false, true, false, true);        // static rewrite, deploy path, naive runtime

  private final boolean remoteLambda;
  private final boolean staticRewrite;
  private final boolean devPathManager;
  private final boolean nativeRuntime;

  RunMode(boolean remoteLambda, boolean staticRewrite, boolean devPathManager,
          boolean nativeRuntime) {
    this.remoteLambda = remoteLambda;
    this.staticRewrite = staticRewrite;
    this.devPathManager = devPathManager;
    this.nativeRuntime = nativeRuntime;
  }

  /**
   * Getter method for property <tt>remoteLambda</tt>.
   *
   * @return property value of remoteLambda
   */
  public boolean isRemoteLambda() {
    return remoteLambda;
  }

  /**
   * Getter method for property <tt>staticRewrite</tt>.
   *
   * @return property value of staticRewrite
   */
  public boolean isStaticRewrite() {
    return staticRewrite;
  }

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
