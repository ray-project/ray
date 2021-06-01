package io.ray.serve;

import java.io.Serializable;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

/**
 * Configuration options for a backend, to be set by the user.
 */
public class BackendConfig implements Serializable {
  
  private static final long serialVersionUID = 244486384449779141L;

  /**
   * The number of processes to start up that will handle requests to this backend. Defaults to 1.
   */
  private int numReplicas = 1;

  /**
   * The maximum number of queries that will be sent to a replica of this backend without receiving
   * a response. Defaults to 100.
   */
  private int maxConcurrentQueries;

  /**
   * Arguments to pass to the reconfigure method of the backend. The reconfigure method is called if
   * user_config is not None.
   */
  private Object userConfig;

  /**
   * Duration that backend workers will wait until there is no more work to be done before shutting
   * down. Defaults to 2s.
   */
  private long experimentalGracefulShutdownWaitLoopS = 2;

  /**
   * Controller waits for this duration to forcefully kill the replica for shutdown. Defaults to
   * 20s.
   */
  private long experimentalGracefulShutdownTimeoutS = 20;

  @Override
  public String toString() {
    return ReflectionToStringBuilder.toString(this);
  }

  public int getNumReplicas() {
    return numReplicas;
  }

  public void setNumReplicas(int numReplicas) {
    this.numReplicas = numReplicas;
  }

  public int getMaxConcurrentQueries() {
    return maxConcurrentQueries;
  }

  public void setMaxConcurrentQueries(int maxConcurrentQueries) {
    this.maxConcurrentQueries = maxConcurrentQueries;
  }

  public Object getUserConfig() {
    return userConfig;
  }

  public void setUserConfig(Object userConfig) {
    this.userConfig = userConfig;
  }

  public long getExperimentalGracefulShutdownWaitLoopS() {
    return experimentalGracefulShutdownWaitLoopS;
  }

  public void setExperimentalGracefulShutdownWaitLoopS(long experimentalGracefulShutdownWaitLoopS) {
    this.experimentalGracefulShutdownWaitLoopS = experimentalGracefulShutdownWaitLoopS;
  }

  public long getExperimentalGracefulShutdownTimeoutS() {
    return experimentalGracefulShutdownTimeoutS;
  }

  public void setExperimentalGracefulShutdownTimeoutS(long experimentalGracefulShutdownTimeoutS) {
    this.experimentalGracefulShutdownTimeoutS = experimentalGracefulShutdownTimeoutS;
  }


}
