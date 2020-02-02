package org.ray.api.options;

import java.util.HashMap;
import java.util.Map;
import org.ray.api.LifeCycleGroup;
import org.ray.api.SlotSet;

/**
 * The options for creating actor.
 */
public class ActorCreationOptions extends BaseTaskOptions {

  public static final int NO_RECONSTRUCTION = 0;
  public static final int INFINITE_RECONSTRUCTION = (int) Math.pow(2, 30);

  public final int maxReconstructions;

  public final String jvmOptions;

  public final int maxConcurrency;

  public final SlotSet slotSet;

  public final LifeCycleGroup lifeCycleGroup;

  private ActorCreationOptions(Map<String, Double> resources, int maxReconstructions,
                               String jvmOptions, int maxConcurrency, SlotSet slotSet,
                               LifeCycleGroup lifeCycleGroup) {
    super(resources);
    this.maxReconstructions = maxReconstructions;
    this.jvmOptions = jvmOptions;
    this.maxConcurrency = maxConcurrency;
    this.slotSet = slotSet;
    this.lifeCycleGroup = lifeCycleGroup;
  }

  /**
   * The inner class for building ActorCreationOptions.
   */
  public static class Builder {

    private Map<String, Double> resources = new HashMap<>();
    private int maxReconstructions = NO_RECONSTRUCTION;
    private String jvmOptions = null;
    private int maxConcurrency = 1;
    private SlotSet slotSet = null;
    private LifeCycleGroup lifeCycleGroup = null;

    public Builder setResources(Map<String, Double> resources) {
      this.resources = resources;
      return this;
    }

    public Builder setMaxReconstructions(int maxReconstructions) {
      this.maxReconstructions = maxReconstructions;
      return this;
    }

    public Builder setJvmOptions(String jvmOptions) {
      this.jvmOptions = jvmOptions;
      return this;
    }

    // The max number of concurrent calls to allow for this actor.
    //
    // The max concurrency defaults to 1 for threaded execution.
    // Note that the execution order is not guaranteed when max_concurrency > 1.
    public Builder setMaxConcurrency(int maxConcurrency) {
      if (maxConcurrency <= 0) {
        throw new IllegalArgumentException("maxConcurrency must be greater than 0.");
      }

      this.maxConcurrency = maxConcurrency;
      return this;
    }

    public Builder setSlotSet(SlotSet slotSet) {
      this.slotSet = slotSet;
      return this;
    }

    public Builder setLifeCycleGroup(LifeCycleGroup lifeCycleGroup) {
      this.lifeCycleGroup = lifeCycleGroup;
      return this;
    }

    public ActorCreationOptions createActorCreationOptions() {
      return new ActorCreationOptions(
          resources, maxReconstructions, jvmOptions, maxConcurrency, slotSet, lifeCycleGroup);
    }
  }

}
