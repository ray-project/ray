package io.ray.runtime.util;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import java.util.HashMap;

/** The utility class to read system config from native code. */
public class SystemConfig {

  private static Gson gson = new Gson();

  /// A cache to avoid duplicated reading via JNI.
  private static HashMap<String, Object> cachedConfigs = new HashMap<>();

  public static synchronized Object get(String key) {
    if (cachedConfigs.containsKey(key)) {
      return cachedConfigs.get(key);
    }

    Object val = gson.fromJson(nativeGetSystemConfig(key), Object.class);
    Preconditions.checkNotNull(val);
    cachedConfigs.put(key, val);
    return val;
  }

  public static boolean bootstrapWithGcs() {
    return ((Boolean) SystemConfig.get("bootstrap_with_gcs")).booleanValue();
  }

  private static native String nativeGetSystemConfig(String key);
}
