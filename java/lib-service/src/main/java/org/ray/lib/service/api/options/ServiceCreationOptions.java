package org.ray.lib.service.api.options;

import org.ray.api.options.ActorCreationOptions;
import org.ray.api.options.ActorGroupOptions;

/**
 * The options for creating a Service.
 */
public class ServiceCreationOptions {

  /**
   * The unique name of this Service.
   */
  public final String name;

  /**
   * Indicates how many Actors in this Service.
   */
  public final int actorCount;

  /**
   * The options for creating the underlying Actors.
   */
  public final ActorCreationOptions actorCreationOptions;

  /**
   * The options for creating the underlying Actor Group.
   */
  public final ActorGroupOptions actorGroupOptions;

  private ServiceCreationOptions(String name, int actorCount,
      ActorCreationOptions actorCreationOptions, ActorGroupOptions actorGroupOptions) {
    this.name = name;
    this.actorCount = actorCount;
    this.actorCreationOptions = actorCreationOptions;
    this.actorGroupOptions = actorGroupOptions;
  }

  /**
   * The inner class for building ServiceCreationOptions.
   */
  public static class Builder {

    private String name = "";
    private int actorCount = 1;
    private ActorCreationOptions actorCreationOptions = null;
    private ActorGroupOptions actorGroupOptions = null;

    private Builder setName(String name) {
      this.name = name;
      return this;
    }

    public Builder setActorCount(int actorCount) {
      this.actorCount = actorCount;
      return this;
    }

    public Builder setActorCreationOptions(ActorCreationOptions actorCreationOptions) {
      this.actorCreationOptions = actorCreationOptions;
      return this;
    }

    public Builder setActorGroupOptions(ActorGroupOptions actorGroupOptions) {
      this.actorGroupOptions = actorGroupOptions;
      return this;
    }

    public ServiceCreationOptions createServiceCreationOptions() {
      return new ServiceCreationOptions(name, actorCount, actorCreationOptions, actorGroupOptions);
    }
  }
}
