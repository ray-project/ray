package org.ray.runtime.util;

import com.google.common.base.Preconditions;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

public class BinaryFileUtil {
  public static final String REDIS_SERVER_BINARY_NAME = "redis-server";

  public static final String PLASMA_STORE_SERVER_BINARY_NAME = "plasma_store_server";

  public static final String RAYLET_BINARY_NAME = "raylet";

  public static final String REDIS_MODULE_LIBRARY_NAME = "libray_redis_module.so";

  public static final String CORE_WORKER_JAVA_LIBRARY = System.mapLibraryName("core_worker_library_java");

  public static String getFilePath(String destDir, String fileName) {
    return String.format("%s/%s", destDir, fileName);
  }

  public static void prepareFilesTo(String destDir) {
    dumpFileFromJar(REDIS_SERVER_BINARY_NAME, destDir);
    dumpFileFromJar(PLASMA_STORE_SERVER_BINARY_NAME, destDir);
    dumpFileFromJar(RAYLET_BINARY_NAME, destDir);
    dumpFileFromJar(REDIS_MODULE_LIBRARY_NAME, destDir);
    dumpFileFromJar(CORE_WORKER_JAVA_LIBRARY, destDir);
  }

  private static void dumpFileFromJar(String fileName, String destDir) {
    File file = new File(String.format("%s/%s", destDir, fileName));

    try (InputStream in = FileUtil.class.getResourceAsStream("/" + fileName)) {
      Preconditions.checkNotNull(in, "{} doesn't exist.", fileName);
      Files.copy(in, Paths.get(file.getCanonicalPath()), StandardCopyOption.REPLACE_EXISTING);

    } catch (IOException e) {
      throw new RuntimeException("Couldn't get temp file from resource " + fileName, e);
    }
  }
}
