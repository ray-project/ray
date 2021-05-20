package io.ray.serve;

import java.io.Serializable;
import java.util.Map;

/**
 * BackendConfig.
 */
public class BackendConfig implements Serializable {
  
  private static final long serialVersionUID = 6356109792183539217L;

  private int numReplicas;

  private int maxConcurrentQueries;

  private Map<String, String> userConfig;

  private float experimentalGracefulShutdownWaitLoop;

  private float experimentalGracefulShutdownTimeout;

}
