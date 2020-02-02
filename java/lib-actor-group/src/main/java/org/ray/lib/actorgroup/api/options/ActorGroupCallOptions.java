package org.ray.lib.actorgroup.api.options;

import org.ray.api.options.CallOptions;

/**
 * The options for calling an Actor group.
 */
public class ActorGroupCallOptions {

  public final CallOptions callOptions;

  public final LoadBalancingStrategy loadBalancingStrategy;

  private ActorGroupCallOptions(CallOptions callOptions, LoadBalancingStrategy loadBalancingStrategy) {
    this.callOptions = callOptions;
    this.loadBalancingStrategy = loadBalancingStrategy;
  }

  /**
   * The inner class for building ActorGroupCallOptions.
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

    public ActorGroupCallOptions createActorGroupCallOptions() {
      return new ActorGroupCallOptions(callOptions, loadBalancingStrategy);
    }
  }

}
