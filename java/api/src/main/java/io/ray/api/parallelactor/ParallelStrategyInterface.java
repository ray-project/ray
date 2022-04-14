package io.ray.api.parallelactor;

/**
 * The interface for parallel strategy.
 *
 * <p>If you'd like to implement a new parallel strategy, you should implements this interface and
 * override the methods.
 */
public interface ParallelStrategyInterface {

  /** Get the total number of the parallels. */
  int getParallelNum();

  /**
   * Define how the strategy works. You should give the next index according to your own strategy.
   */
  int getNextIndex();

  /**
   * Reset the state of strategy. This is used for passing a parallel actor to another work. Due to
   * the new worker shouldn't use the old state, we should define this method to reset it.
   */
  void reset();
}
