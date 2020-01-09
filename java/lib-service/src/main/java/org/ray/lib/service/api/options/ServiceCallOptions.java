package org.ray.lib.service.api.options;

import org.ray.api.options.CallOptions;
import org.ray.lib.service.api.LoadBalancingStrategy;

/**
 * The options for calling a Service.
 */
public class ServiceCallOptions {

  public final CallOptions callOptions;
  public final LoadBalancingStrategy loadBalancingStrategy;

  private ServiceCallOptions(CallOptions callOptions, LoadBalancingStrategy loadBalancingStrategy) {
    this.callOptions = callOptions;
    this.loadBalancingStrategy = loadBalancingStrategy;
  }

  /**
   * The inner class for building ServiceCallOptions.
   */
  public static class Builder {

    private CallOptions callOptions = null;
    private LoadBalancingStrategy loadBalancingStrategy = LoadBalancingStrategy.ROUND_ROBIN;

    public Builder setCallOptions(CallOptions callOptions) {
      this.callOptions = callOptions;
      return this;
    }

    public Builder setLoadBalancingStrategy(LoadBalancingStrategy loadBalancingStrategy) {
      this.loadBalancingStrategy = loadBalancingStrategy;
      return this;
    }

    public ServiceCallOptions createServiceCallOptions() {
      return new ServiceCallOptions(callOptions, loadBalancingStrategy);
    }
  }

}
