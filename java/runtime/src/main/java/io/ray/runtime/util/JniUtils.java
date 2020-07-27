package io.ray.runtime.util;

import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.sun.jna.NativeLibrary;
import io.ray.runtime.config.RayConfig;
import java.io.File;
import java.lang.reflect.Field;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JniUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(JniUtils.class);
  private static Set<String> loadedLibs = Sets.newHashSet();

  /**
   * Loads the native library specified by the <code>libraryName</code> argument.
   * The <code>libraryName</code> argument must not contain any platform specific
   * prefix, file extension or path.
   *
   * @param libraryName the name of the library.
   */
  public static synchronized void loadLibrary(String libraryName) {
    loadLibrary(libraryName, false);
  }

  /**
   * Loads the native library specified by the <code>libraryName</code> argument.
   * The <code>libraryName</code> argument must not contain any platform specific
   * prefix, file extension or path.
   *
   * @param libraryName   the name of the library.
   * @param exportSymbols export symbols of library so that it can be used by other libs.
   */
  public static synchronized void loadLibrary(String libraryName, boolean exportSymbols) {
    if (!loadedLibs.contains(libraryName)) {
      LOGGER.debug("Loading native library {}.", libraryName);
      // Load native library.
      String fileName = System.mapLibraryName(libraryName);
      final String sessionDir = RayConfig.getInstance().sessionDir;
      final File file = BinaryFileUtil.getFile(sessionDir, fileName);

      if (exportSymbols) {
        // Expose library symbols using RTLD_GLOBAL which may be depended by other shared
        // libraries.
        NativeLibrary.getInstance(file.getAbsolutePath());
      }
      System.load(file.getAbsolutePath());
      LOGGER.debug("Native library loaded.");
      resetLibraryPath(file.getAbsolutePath());
      loadedLibs.add(libraryName);
    }
  }

  /**
   * This is a hack to reset library path at runtime. Please don't use it outside of ray
   */
  public static synchronized void resetLibraryPath(String libPath) {
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
