package io.ray.streaming.api.function;

import java.io.Serializable;

/** Interface of streaming functions. */
public interface Function extends Serializable {

  /**
   * This method will be called periodically by framework, you should return a a serializable object
   * which represents function state, framework will help you to serialize this object, save it to
   * storage, and load it back when in fail-over through. {@link
   * Function#loadCheckpoint(Serializable)}.
   *
   * <p>Returns A serializable object which represents function state.
   */
  default Serializable saveCheckpoint() {
    return null;
  }

  /**
   * This method will be called by framework when a worker died and been restarted. We will pass the
   * last object you returned in {@link Function#saveCheckpoint()} when doing checkpoint, you are
   * responsible to load this object back to you function.
   *
   * @param checkpointObject the last object you returned in {@link Function#saveCheckpoint()}
   */
  default void loadCheckpoint(Serializable checkpointObject) {}
}
