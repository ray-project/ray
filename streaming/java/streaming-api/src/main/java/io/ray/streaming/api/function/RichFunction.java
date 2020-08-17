package io.ray.streaming.api.function;

import io.ray.streaming.api.context.RuntimeContext;

/**
 * An interface for all user-defined functions to define the life cycle methods of the
 * functions, and access the task context where the functions get executed.
 */
public interface RichFunction extends Function {

  /**
   * Initialization method for user function which called before the first call to the user
   * function.
   * @param runtimeContext runtime context
   */
  void open(RuntimeContext runtimeContext);

  /**
   * Tear-down method for the user function which called after the last call to
   * the user function.
   */
  void close();

}
