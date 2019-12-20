package org.ray.streaming.runtime;

import org.ray.runtime.RayNativeRuntime;
import org.ray.runtime.util.JniUtils;

public class TestHelper {

  public static void loadNativeLibraries() {
    // load core_worker_library_java before load streaming_java
    try {
      Class.forName(RayNativeRuntime.class.getName());
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    JniUtils.loadLibrary("streaming_java");
  }

}
