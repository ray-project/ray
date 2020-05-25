package io.ray.streaming.runtime.master.resourcemanager.strategy;

import io.ray.streaming.runtime.config.types.ResourceAssignStrategyType;
import io.ray.streaming.runtime.master.resourcemanager.strategy.impl.PipelineFirstStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResourceAssignStrategyFactory {

  private static final Logger LOG = LoggerFactory.getLogger(ResourceAssignStrategyFactory.class);

  public static ResourceAssignStrategy getStrategy(final ResourceAssignStrategyType type) {
    ResourceAssignStrategy strategy = null;
    LOG.info("Slot assign strategy is: {}.", type);
    switch (type) {
      case PIPELINE_FIRST_STRATEGY:
        strategy = new PipelineFirstStrategy();
        break;
      default:
        throw new RuntimeException("strategy config error, no impl found for " + strategy);
    }
    return strategy;
  }
}
