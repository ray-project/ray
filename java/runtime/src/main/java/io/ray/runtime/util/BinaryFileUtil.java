package io.ray.runtime.util;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.apache.commons.io.FileUtils;

public class BinaryFileUtil {
  // We use a path here because the top-level Bazel target is an alias
  public static final String REDIS_SERVER_BINARY_PATH =
      "external/com_github_antirez_redis/redis-server";

  public static final String GCS_SERVER_BINARY_NAME = "gcs_server";

  public static final String PLASMA_STORE_SERVER_BINARY_NAME = "plasma_store_server";

  public static final String RAYLET_BINARY_NAME = "raylet";

  public static final String REDIS_MODULE_LIBRARY_NAME = "libray_redis_module.so";

  public static final String CORE_WORKER_JAVA_LIBRARY =
      System.mapLibraryName("core_worker_library_java");

  /**
   * Extract a resource file to <code>destDir</code>.
   * Note that this a process-safe operation. If multi processes extract the file to same
   * directory concurrently, this operation will be protected by a file lock.
   *
   * @param destDir  a directory to extract resource file to
   * @param filePath resource file path
   * @return extracted resource file
   */
  public static File getFile(String destDir, String filePath) {
    String fileName = new File(filePath).getName();
    final File dir = new File(destDir);
    if (!dir.exists()) {
      try {
        FileUtils.forceMkdir(dir);
      } catch (IOException e) {
        throw new RuntimeException("Couldn't make directory: " + dir.getAbsolutePath(), e);
      }
    }
    String lockFilePath = destDir + File.separator + "file_lock";
    try (FileLock ignored = new RandomAccessFile(lockFilePath, "rw")
        .getChannel().lock()) {
      File file = new File(String.format("%s/%s", destDir, fileName));
      if (file.exists()) {
        return file;
      }

      // File does not exist.
      try (InputStream is = BinaryFileUtil.class.getResourceAsStream("/" + filePath)) {
        Preconditions.checkNotNull(is, "{} doesn't exist.", filePath);
        Files.copy(is, Paths.get(file.getCanonicalPath()));
      } catch (IOException e) {
        throw new RuntimeException("Couldn't get temp file from resource " + filePath, e);
      }
      return file;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
