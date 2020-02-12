package org.ray.runtime.util;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.apache.commons.io.FileUtils;

public class BinaryFileUtil {
  public static final String REDIS_SERVER_BINARY_NAME = "redis-server";

  public static final String GCS_SERVER_BINARY_NAME = "gcs_server";

  public static final String PLASMA_STORE_SERVER_BINARY_NAME = "plasma_store_server";

  public static final String RAYLET_BINARY_NAME = "raylet";

  public static final String REDIS_MODULE_LIBRARY_NAME = "libray_redis_module.so";

  public static final String CORE_WORKER_JAVA_LIBRARY =
      System.mapLibraryName("core_worker_library_java");

  public static File getFile(String destDir, String fileName) {
    File file = new File(String.format("%s/%s", destDir, fileName));
    if (file.exists()) {
      return file;
    }

    final File dir = file.getParentFile();
    try {
      if (!dir.exists()) {
        FileUtils.forceMkdir(dir);
      }
    } catch (IOException e) {
      throw new RuntimeException("Couldn't make directory: " + dir.getAbsolutePath(), e);
    }
    // File does not exist.
    try (InputStream is = BinaryFileUtil.class.getResourceAsStream("/" + fileName)) {
      Preconditions.checkNotNull(is, "{} doesn't exist.", fileName);
      Files.copy(is, Paths.get(file.getCanonicalPath()));
    } catch (IOException e) {
      throw new RuntimeException("Couldn't get temp file from resource " + fileName, e);
    }
    return file;
  }
}
