package org.ray.streaming.runtime.util;

import java.lang.management.ManagementFactory;
import org.ray.runtime.RayNativeRuntime;
import org.ray.runtime.util.JniUtils;

public class EnvUtil {

  public static String getJvmPid() {
    return ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
  }

  public static void loadNativeLibraries() {
    // Explicitly load `RayNativeRuntime`, to make sure `core_worker_library_java`
    // is loaded before `streaming_java`.
    try {
      Class.forName(RayNativeRuntime.class.getName());
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    JniUtils.loadLibrary("streaming_java");
  }

}
