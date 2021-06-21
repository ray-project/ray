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
import org.apache.commons.lang3.SystemUtils;

public class BinaryFileUtil {

  public static final String CORE_WORKER_JAVA_LIBRARY = "core_worker_library_java";

  /**
   * Extract a platform-native resource file to <code>destDir</code>. Note that this a process-safe
   * operation. If multi processes extract the file to same directory concurrently, this operation
   * will be protected by a file lock.
   *
   * @param destDir a directory to extract resource file to
   * @param fileName resource file name
   * @return extracted resource file
   */
  public static File getNativeFile(String destDir, String fileName) {
    final File dir = new File(destDir);
    if (!dir.exists()) {
      try {
        FileUtils.forceMkdir(dir);
      } catch (IOException e) {
        throw new RuntimeException("Couldn't make directory: " + dir.getAbsolutePath(), e);
      }
    }
    String lockFilePath = destDir + File.separator + "file_lock";
    try (FileLock ignored = new RandomAccessFile(lockFilePath, "rw").getChannel().lock()) {
      String resourceDir;
      if (SystemUtils.IS_OS_MAC) {
        resourceDir = "native/darwin/";
      } else if (SystemUtils.IS_OS_LINUX) {
        resourceDir = "native/linux/";
      } else {
        throw new UnsupportedOperationException("Unsupported os " + SystemUtils.OS_NAME);
      }
      String resourcePath = resourceDir + fileName;
      File file = new File(String.format("%s/%s", destDir, fileName));
      if (file.exists()) {
        return file;
      }

      // File does not exist.
      try (InputStream is = BinaryFileUtil.class.getResourceAsStream("/" + resourcePath)) {
        Preconditions.checkNotNull(is, "{} doesn't exist.", resourcePath);
        Files.copy(is, Paths.get(file.getCanonicalPath()));
      } catch (IOException e) {
        throw new RuntimeException("Couldn't get temp file from resource " + resourcePath, e);
      }
      return file;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
