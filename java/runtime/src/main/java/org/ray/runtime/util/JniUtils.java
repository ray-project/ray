package org.ray.runtime.util;

import com.google.common.base.Strings;
import com.sun.jna.NativeLibrary;
import java.lang.reflect.Field;
import org.ray.runtime.RayNativeRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JniUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(JniUtils.class);

  public static void loadLibrary(String libraryName) {
    loadLibrary(libraryName, false);
  }

  public static void loadLibrary(String libraryName, boolean exportSymbols) {
    try {
      System.loadLibrary(libraryName);
    } catch (UnsatisfiedLinkError error) {
      LOGGER.debug("Loading native library {}.", libraryName);
      // Load native library.
      String fileName = System.mapLibraryName(libraryName);
      String libPath = null;
      try (FileUtil.TempFile libFile = FileUtil.getTempFileFromResource(fileName)) {
        libPath = libFile.getFile().getAbsolutePath();
        if (exportSymbols) {
          // Expose library symbols using RTLD_GLOBAL which may be depended by other shared
          // libraries.
          NativeLibrary.getInstance(libFile.getFile().getAbsolutePath());
        }
        System.load(libPath);
      }
      LOGGER.debug("Native library loaded.");
      resetLibraryPath(libPath);
    }
  }

  /**
   * @see RayNativeRuntime resetLibraryPath
   */
  public static void resetLibraryPath(String libPath) {
    if (Strings.isNullOrEmpty(libPath)) {
      return;
    }
    String path = System.getProperty("java.library.path");
    String separator = System.getProperty("path.separator");
    if (Strings.isNullOrEmpty(path)) {
      path = "";
    } else {
      path += separator;
    }
    path += String.join(separator, libPath);

    // This is a hack to reset library path at runtime,
    // see https://stackoverflow.com/questions/15409223/.
    System.setProperty("java.library.path", path);
    // Set sys_paths to null so that java.library.path will be re-evaluated next time it is needed.
    final Field sysPathsField;
    try {
      sysPathsField = ClassLoader.class.getDeclaredField("sys_paths");
      sysPathsField.setAccessible(true);
      sysPathsField.set(null, null);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      LOGGER.error("Failed to set library path.", e);
    }
  }
}
