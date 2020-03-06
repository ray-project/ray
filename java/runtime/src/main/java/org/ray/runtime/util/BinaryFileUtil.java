package org.ray.runtime.util;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.SystemUtils;

public class BinaryFileUtil {
  public static final String REDIS_SERVER_BINARY_NAME = "redis-server";

  public static final String GCS_SERVER_BINARY_NAME = "gcs_server";

  public static final String PLASMA_STORE_SERVER_BINARY_NAME = "plasma_store_server";

  public static final String RAYLET_BINARY_NAME = "raylet";

  public static final String REDIS_MODULE_LIBRARY_NAME = "libray_redis_module.so";

  public static final String CORE_WORKER_JAVA_LIBRARY =
      System.mapLibraryName("core_worker_library_java");

  /**
   * Get the full name of the specific name. This is used to load different
   * binaries and libraries on different platform.
   */
  private static String getNameWithPlatformSuffix(String namePrefix) {
    if (namePrefix.contains(".")) {
      String[] tuple = namePrefix.split("\\.");
      Preconditions.checkState(tuple.length == 2);
      return tuple[0] + getPlatformSuffix() + "." + tuple[1];
    } else {
      return namePrefix + getPlatformSuffix();
    }
  }

  /**
   * Get the platform signed string.
   */
  private static String getPlatformSuffix() {
    if (SystemUtils.IS_OS_MAC_OSX) {
      return "_os_mac_osx";
    } else if (SystemUtils.IS_OS_LINUX) {
      return "_os_linux";
    } else {
      throw new NotImplementedException("We have not supported on current platform.");
    }
  }

  /**
   * Extract a resource file to <code>destDir</code>.
   * Note that this a process-safe operation. If multi processes extract the file to same
   * directory concurrently, this operation will be protected by a file lock.
   *
   * @param destDir  a directory to extract resource file to
   * @param fileName resource file name
   * @return extracted resource file
   */
  public static File getFile(String destDir, String fileName) {
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
      boolean platformRelatedFileExists = false;
      try (InputStream is = BinaryFileUtil.class.getResourceAsStream(
        "/" + getNameWithPlatformSuffix(fileName))) {
        if (is != null) {
          Files.copy(is, Paths.get(file.getCanonicalPath()));
          platformRelatedFileExists = true;
        }
      } catch (IOException e) {
        throw new RuntimeException("Couldn't get temp file from resource " + fileName, e);
      }

      if (!platformRelatedFileExists) {
        try (InputStream is = BinaryFileUtil.class.getResourceAsStream("/" + fileName)) {
          Preconditions.checkNotNull(is, "File doesn't exist.");
          Files.copy(is, Paths.get(file.getCanonicalPath()));
        } catch (IOException e) {
          throw new RuntimeException("Couldn't get temp file from resource " + fileName, e);
        }
      }

      return file;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
