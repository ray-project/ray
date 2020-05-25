package io.ray.streaming.runtime.config.master;

import io.ray.streaming.runtime.config.Config;

/**
 * Job resource management config.
 */
public interface ResourceConfig extends Config {

  /**
   * Number of actors per container.
   */
  String MAX_ACTOR_NUM_PER_CONTAINER = "streaming.container.per.max.actor";

  /**
   * The interval between detecting ray cluster nodes.
   */
  String CONTAINER_RESOURCE_CHECk_INTERVAL_SECOND = "streaming.resource.check.interval.second";

  /**
   * CPU use by per task.
   */
  String TASK_RESOURCE_CPU = "streaming.task.resource.cpu";

  /**
   * Memory use by each task
   */
  String TASK_RESOURCE_MEM = "streaming.task.resource.mem";

  /**
   * Whether to enable CPU limit in resource control.
   */
  String TASK_RESOURCE_CPU_LIMIT_ENABLE = "streaming.task.resource.cpu.limitation.enable";

  /**
   * Whether to enable memory limit in resource control.
   */
  String TASK_RESOURCE_MEM_LIMIT_ENABLE = "streaming.task.resource.mem.limitation.enable";

  /**
   * Number of cpu per task.
   */
  @DefaultValue(value = "1.0")
  @Key(value = TASK_RESOURCE_CPU)
  double taskCpuResource();

  /**
   * Memory size used by each task.
   */
  @DefaultValue(value = "2.0")
  @Key(value = TASK_RESOURCE_MEM)
  double taskMemResource();

  /**
   * Whether to enable CPU limit in resource control.
   */
  @DefaultValue(value = "true")
  @Key(value = TASK_RESOURCE_CPU_LIMIT_ENABLE)
  boolean isTaskCpuResourceLimit();

  /**
   * Whether to enable memory limit in resource control.
   */
  @DefaultValue(value = "true")
  @Key(value = TASK_RESOURCE_MEM_LIMIT_ENABLE)
  boolean isTaskMemResourceLimit();

  /**
   * Number of actors per container.
   */
  @DefaultValue(value = "500")
  @Key(MAX_ACTOR_NUM_PER_CONTAINER)
  int actorNumPerContainer();

  /**
   * The interval between detecting ray cluster nodes.
   */
  @DefaultValue(value = "1")
  @Key(value = CONTAINER_RESOURCE_CHECk_INTERVAL_SECOND)
  long resourceCheckIntervalSecond();

}
