package org.ray.runtime.util;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Scanner;
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

  public static String getFilename(String logPath) {
    if (logPath != null && !logPath.isEmpty()) {
      int lastPos = logPath.lastIndexOf('/');
      if (lastPos != -1) {
        return logPath.substring(lastPos + 1);
      } else {
        return logPath;
      }
    }

    return null;
  }

  public static boolean deleteFile(String filePath) {
    File file = new File(filePath);
    if (!file.exists()) {
      return true;
    } else {
      if (file.isFile()) {
        return file.delete();
      } else {
        for (File f : file.listFiles()) {
          deleteFile(f.getAbsolutePath());
        }
        return file.delete();
      }
    }
  }

  public static void mkDir(File dir) {
    if (dir.exists()) {
      return;
    }
    if (dir.getParentFile().exists()) {
      dir.mkdir();
    } else {
      mkDir(dir.getParentFile());
      dir.mkdir();
    }
  }

  public static void mkDirAndFile(File file) throws IOException {
    if (file.exists()) {
      return;
    }
    if (!file.getParentFile().exists()) {
      mkDir(file.getParentFile());
    }
    file.createNewFile();
  }

  public static String readResourceFile(String fileName) throws FileNotFoundException {
    ClassLoader classLoader = FileUtil.class.getClassLoader();
    File file = new File(classLoader.getResource(fileName).getFile());
    StringBuilder result = new StringBuilder();
    try (Scanner scanner = new Scanner(file)) {

      //Get file from resources folder

      while (scanner.hasNextLine()) {
        String line = scanner.nextLine();
        result.append(line).append("\n");
      }
      return result.toString();
    }

  }

  public static void overrideFile(String file, String str) throws IOException {
    try (FileWriter fw = new FileWriter(file)) {
      fw.write(str);
    }
  }

  public static boolean createDir(String dirName, boolean failIfExist) {
    File wdir = new File(dirName);
    if (wdir.isFile()) {
      return false;
    }

    if (!wdir.exists()) {
      wdir.mkdirs();
    } else if (failIfExist) {
      return false;
    }

    return true;
  }

  public static void bytesToFile(byte[] bytes, String name) throws IOException {
    Path path = Paths.get(name);
    Files.write(path, bytes);
  }

  public static byte[] fileToBytes(String name) throws IOException {
    Path path = Paths.get(name);
    return Files.readAllBytes(path);
  }

  /**
   * If the given string is the empty string, then the result is the current directory.
   *
   * @param rawDir a path in any legal form, such as a relative path
   * @return the absolute and unique path in String
   */
  public static String getCanonicalDirectory(final String rawDir) throws IOException {
    String dir = rawDir.length() == 0 ? "." : rawDir;

    // create working dir if necessary
    File dd = new File(dir);
    if (!dd.exists()) {
      dd.mkdirs();
    }

    if (!dir.startsWith("/")) {
      dir = dd.getCanonicalPath();
    }

    return dir;
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
