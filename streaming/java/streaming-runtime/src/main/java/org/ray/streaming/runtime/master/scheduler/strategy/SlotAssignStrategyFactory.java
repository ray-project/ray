package org.ray.streaming.runtime.master.scheduler.strategy;

import org.ray.streaming.runtime.config.types.SlotAssignStrategyType;
import org.ray.streaming.runtime.master.scheduler.strategy.impl.PipelineFirstStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SlotAssignStrategyFactory {

  private static final Logger LOG = LoggerFactory.getLogger(SlotAssignStrategyFactory.class);

  public static SlotAssignStrategy getStrategy(final SlotAssignStrategyType type) {
    SlotAssignStrategy strategy = null;
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
