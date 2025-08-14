package io.ray.runtime.util;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import io.ray.runtime.config.RayConfig;
import io.ray.runtime.config.RunMode;
import java.util.HashMap;

/** The utility class to read system config from native code. */
public class SystemConfig {

  private static RayConfig rayConfig = null;

  private static Gson gson = new Gson();

  /// A cache to avoid duplicated reading via JNI.
  private static HashMap<String, Object> cachedConfigs = new HashMap<>();

  /**
   * The string key to use for getting the largest size passed by value. If the size of an
   * argument's serialized data is smaller than this number, the argument will be passed by value.
   * Otherwise it'll be passed by reference.
   */
  public static final String KEY_TO_LARGEST_SIZE_PASS_BY_VALUE = "max_direct_call_object_size";

  public static synchronized void setup(RayConfig config) {
    rayConfig = config;
  }

  public static synchronized Object get(String key) {
    Preconditions.checkNotNull(rayConfig);
    if (rayConfig.runMode == RunMode.LOCAL) {
      // Code path of local mode.
      return getInLocalMode(key);
    }
    // Code path of cluster mode.
    if (cachedConfigs.containsKey(key)) {
      return cachedConfigs.get(key);
    }

    Object val = gson.fromJson(nativeGetSystemConfig(key), Object.class);
    Preconditions.checkNotNull(val);
    cachedConfigs.put(key, val);
    return val;
  }

  public static synchronized long getLargestSizePassedByValue() {
    return ((Double) SystemConfig.get(KEY_TO_LARGEST_SIZE_PASS_BY_VALUE)).longValue();
  }

  private static Object getInLocalMode(String key) {
    if (KEY_TO_LARGEST_SIZE_PASS_BY_VALUE.equals(key)) {
      // Hard code 10K in local mode.
      return 100.0 * 1024;
    }
    throw new RuntimeException(String.format("Unsupported key: %s", key));
  }

  private static native String nativeGetSystemConfig(String key);
}
