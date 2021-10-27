package io.ray.serve;

import com.google.common.base.Preconditions;
import java.io.Serializable;

public class BackendConfig implements Serializable {

  private static final long serialVersionUID = 4037621960087621036L;

  private int numReplicas = 1;

  private int maxConcurrentQueries = 100;

  private Object userConfig;

  private double gracefulShutdownWaitLoopS = 2;

  private double gracefulShutdownTimeoutS = 20;

  private boolean isCrossLanguage;

  private int backendLanguage = 1;

  public int getNumReplicas() {
    return numReplicas;
  }

  public BackendConfig setNumReplicas(int numReplicas) {
    this.numReplicas = numReplicas;
    return this;
  }

  public int getMaxConcurrentQueries() {
    return maxConcurrentQueries;
  }

  public BackendConfig setMaxConcurrentQueries(int maxConcurrentQueries) {
    Preconditions.checkArgument(maxConcurrentQueries >= 0, "max_concurrent_queries must be >= 0");
    this.maxConcurrentQueries = maxConcurrentQueries;
    return this;
  }

  public Object getUserConfig() {
    return userConfig;
  }

  public BackendConfig setUserConfig(Object userConfig) {
    this.userConfig = userConfig;
    return this;
  }

  public double getGracefulShutdownWaitLoopS() {
    return gracefulShutdownWaitLoopS;
  }

  public BackendConfig setGracefulShutdownWaitLoopS(double gracefulShutdownWaitLoopS) {
    this.gracefulShutdownWaitLoopS = gracefulShutdownWaitLoopS;
    return this;
  }

  public double getGracefulShutdownTimeoutS() {
    return gracefulShutdownTimeoutS;
  }

  public BackendConfig setGracefulShutdownTimeoutS(double gracefulShutdownTimeoutS) {
    this.gracefulShutdownTimeoutS = gracefulShutdownTimeoutS;
    return this;
  }

  public boolean isCrossLanguage() {
    return isCrossLanguage;
  }

  public BackendConfig setCrossLanguage(boolean isCrossLanguage) {
    this.isCrossLanguage = isCrossLanguage;
    return this;
  }

  public int getBackendLanguage() {
    return backendLanguage;
  }

  public BackendConfig setBackendLanguage(int backendLanguage) {
    this.backendLanguage = backendLanguage;
    return this;
  }
}
