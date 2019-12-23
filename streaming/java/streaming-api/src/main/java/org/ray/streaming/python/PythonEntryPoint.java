package org.ray.streaming.python;

import org.ray.api.annotation.RayRemote;
import org.ray.streaming.api.context.StreamingContext;
import org.ray.streaming.api.stream.Stream;

/**
 * EntryPoint for streaming python.
 * All calls on DataStream in python will be mapped to DataStream call in java by this
 * PythonEntryPoint using ray calls.
 */
@RayRemote
public class PythonEntryPoint {
  private StreamingContext streamingContext;
  private Stream lastStream;

}
