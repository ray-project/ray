package org.ray.streaming.runtime.config.master;

import org.ray.streaming.runtime.config.Config;

/**
 * Job resource management config.
 */
public interface ResourceConfig extends Config {

  String RESOURCE_KEY_CPU = "CPU";
  String RESOURCE_KEY_MEM = "MEM";

  String MAX_ACTOR_NUM_PER_CONTAINER = "streaming.container.per.max.actor";
  String CONTAINER_RESOURCE_CHECk_INTERVAL_SECOND = "streaming.resource.check.interval.second";

  String TASK_RESOURCE_CPU = "streaming.task.resource.cpu";
  String TASK_RESOURCE_MEM = "streaming.task.resource.mem";
  String TASK_RESOURCE_CPU_LIMIT_ENABLE = "streaming.task.resource.cpu.limitation.enable";
  String TASK_RESOURCE_MEM_LIMIT_ENABLE = "streaming.task.resource.mem.limitation.enable";

  @DefaultValue(value = "1.0")
  @Key(value = TASK_RESOURCE_CPU)
  double taskCpuResource();

  @DefaultValue(value = "2.0")
  @Key(value = TASK_RESOURCE_MEM)
  double taskMemResource();

  @DefaultValue(value = "true")
  @Key(value = TASK_RESOURCE_CPU_LIMIT_ENABLE)
  boolean isTaskCpuResourceLimit();

  @DefaultValue(value = "true")
  @Key(value = TASK_RESOURCE_MEM_LIMIT_ENABLE)
  boolean isTaskMemResourceLimit();

  @DefaultValue(value = "500")
  @Key(MAX_ACTOR_NUM_PER_CONTAINER)
  int maxActorNumPerContainer();

  @DefaultValue(value = "1")
  @Key(value = CONTAINER_RESOURCE_CHECk_INTERVAL_SECOND)
  long resourceCheckIntervalSecond();

}
