package io.ray.streaming.api.function;

import java.io.Serializable;

/**
 * Interface of streaming functions.
 */
public interface Function extends Serializable {

  default void loadCheckpoint(Object checkpointObject, long checkpointId) {

  }

  default Object doCheckpoint(long checkpointId) {
    return null;
  }

}
