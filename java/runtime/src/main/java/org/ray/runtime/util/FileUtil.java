package org.ray.runtime.util;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

public class FileUtil {

  public static class TempFile implements AutoCloseable {

    File file;

    TempFile(String resourceFileName) {
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
    }

    public File getFile() {
      return file;
    }

    @Override
    public void close() {
      Preconditions.checkState(file.delete(), "Couldn't delete file {}", file.getAbsolutePath());
    }
  }

  public static TempFile getTempFileFromResource(String resourceFileName) {
    return new TempFile(resourceFileName);
  }
}

