package io.ray.runtime.util;

import com.google.common.collect.Sets;
import com.sun.jna.NativeLibrary;
import io.ray.runtime.config.RayConfig;
import java.io.File;
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
      final File file = BinaryFileUtil.getNativeFile(sessionDir, fileName);

      if (exportSymbols) {
        // Expose library symbols using RTLD_GLOBAL which may be depended by other shared
        // libraries.
        NativeLibrary.getInstance(file.getAbsolutePath());
      }
      System.load(file.getAbsolutePath());
      LOGGER.debug("Native library loaded.");
      loadedLibs.add(libraryName);
    }
  }

}
