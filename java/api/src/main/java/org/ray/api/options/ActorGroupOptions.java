package org.ray.api.options;

/**
 * The options for an Actor group.
 */
public class ActorGroupOptions {

  public final ActorDistributionStrategy actorDistributionStrategy;
  public final ActorLifecycleStrategy actorLifecycleStrategy;

  private ActorGroupOptions(ActorDistributionStrategy actorDistributionStrategy,
      ActorLifecycleStrategy actorLifecycleStrategy) {
    this.actorDistributionStrategy = actorDistributionStrategy;
    this.actorLifecycleStrategy = actorLifecycleStrategy;
  }

  /**
   * The inner class for building ActorGroupOptions.
   */
  public static class Builder {

    private ActorDistributionStrategy actorDistributionStrategy = ActorDistributionStrategy.RANDOM;
    private ActorLifecycleStrategy actorLifecycleStrategy = ActorLifecycleStrategy.INDEPENDENT;

    public Builder setActorDistributionStrategy(
        ActorDistributionStrategy actorDistributionStrategy) {
      this.actorDistributionStrategy = actorDistributionStrategy;
      return this;
    }

    public Builder setActorLifecycleStrategy(ActorLifecycleStrategy actorLifecycleStrategy) {
      this.actorLifecycleStrategy = actorLifecycleStrategy;
      return this;
    }

    public ActorGroupOptions createActorGroupOptions() {
      return new ActorGroupOptions(actorDistributionStrategy, actorLifecycleStrategy);
    }
  }
}
