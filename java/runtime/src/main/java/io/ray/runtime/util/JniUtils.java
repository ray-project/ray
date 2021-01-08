package io.ray.runtime.util;

import com.google.common.collect.Sets;
import com.sun.jna.NativeLibrary;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JniUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(JniUtils.class);
  private static Set<String> loadedLibs = Sets.newHashSet();
  private static String defaultDestDir;

  /**
   * Loads the native library specified by the <code>libraryName</code> argument. The <code>
   * libraryName</code> argument must not contain any platform specific prefix, file extension or
   * path.
   *
   * @param libraryName the name of the library.
   */
  public static synchronized void loadLibrary(String libraryName) {
    loadLibrary(getDefaultDestDir(), libraryName);
  }

  /**
   * Loads the native library specified by the <code>libraryName</code> argument. The <code>
   * libraryName</code> argument must not contain any platform specific prefix, file extension or
   * path.
   *
   * @param libraryName the name of the library.
   * @param exportSymbols export symbols of library so that it can be used by other libs.
   */
  public static synchronized void loadLibrary(String libraryName, boolean exportSymbols) {
    loadLibrary(getDefaultDestDir(), libraryName, exportSymbols);
  }

  /**
   * Loads the native library specified by the <code>libraryName</code> argument. The <code>
   * libraryName</code> argument must not contain any platform specific prefix, file extension or
   * path.
   *
   * @param destDir The destination dir the library to be extracted.
   * @param libraryName the name of the library.
   */
  public static synchronized void loadLibrary(String destDir, String libraryName) {
    loadLibrary(destDir, libraryName, false);
  }

  /**
   * Loads the native library specified by the <code>libraryName</code> argument. The <code>
   * libraryName</code> argument must not contain any platform specific prefix, file extension or
   * path.
   *
   * @param destDir The destination dir the library to be extracted.
   * @param libraryName the name of the library.
   * @param exportSymbols export symbols of library so that it can be used by other libs.
   */
  public static synchronized void loadLibrary(
      String destDir, String libraryName, boolean exportSymbols) {
    if (!loadedLibs.contains(libraryName)) {
      LOGGER.debug("Loading native library {}.", libraryName);
      // Load native library.
      String fileName = System.mapLibraryName(libraryName);
      final File file = BinaryFileUtil.getNativeFile(destDir, fileName);

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

  /** Cache the result so that multiple calls return the same dest dir. */
  private static synchronized String getDefaultDestDir() {
    if (defaultDestDir == null) {
      try {
        defaultDestDir = Files.createTempDirectory("native_libs").toString();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return defaultDestDir;
  }
}
