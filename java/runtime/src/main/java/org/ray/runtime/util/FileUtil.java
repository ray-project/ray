package org.ray.runtime.util;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileUtil.class);

  /**
   * Represents a temp file.
   *
   * This class implements the `AutoCloseable` interface. It can be used in a `try-with-resource`
   * block. When exiting the block, the temp file will be automatically removed.
   */
  public static class TempFile implements AutoCloseable {

    File file;

    TempFile(File file) {
      this.file = file;
    }

    public File getFile() {
      return file;
    }

    @Override
    public void close() {
      if (!file.delete()) {
        LOGGER.warn("Couldn't delete temp file {}", file.getAbsolutePath());
      }
    }
  }

  /**
   * Get a temp file from resource.
   *
   * @param resourceFileName File name.
   * @return A `TempFile` object.
   */
  public static TempFile getTempFileFromResource(String resourceFileName) {
    File file;
    try {
      file = File.createTempFile(resourceFileName, "");
    } catch (IOException e) {
      throw new RuntimeException("Couldn't create temp file " + resourceFileName, e);
    }

    try (InputStream in = FileUtil.class.getResourceAsStream("/" + resourceFileName)) {
      Preconditions.checkNotNull(in, "{} doesn't exist.", resourceFileName);
      Files.copy(in, Paths.get(file.getCanonicalPath()), StandardCopyOption.REPLACE_EXISTING);

    } catch (IOException e) {
      throw new RuntimeException("Couldn't get temp file from resource " + resourceFileName, e);
    }

    return new TempFile(file);
  }
}

