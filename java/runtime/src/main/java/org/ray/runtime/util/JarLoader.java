package org.ray.runtime.util;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.ray.runtime.util.logger.RayLog;

/**
 * load and unload jars from a dir.
 */
public class JarLoader {

  public static URLClassLoader loadJars(String dir, boolean explicitLoad) {
    // get all jars
    Collection<File> jars = FileUtils.listFiles(
        new File(dir),
        new RegexFileFilter(".*\\.jar"),
        DirectoryFileFilter.DIRECTORY
    );
    return loadJar(jars, explicitLoad);
  }

  public static void unloadJars(ClassLoader loader) {
    // now do nothing, if no ref to the loader and loader's class.
    // they would be gc.
  }

  private static URLClassLoader loadJar(Collection<File> appJars, boolean explicitLoad) {
    List<JarFile> jars = new ArrayList<>();
    List<URL> urls = new ArrayList<>();

    for (File appJar : appJars) {
      try {
        RayLog.core.info("load jar " + appJar.getAbsolutePath());
        JarFile jar = new JarFile(appJar.getAbsolutePath());
        jars.add(jar);
        urls.add(appJar.toURI().toURL());
      } catch (IOException e) {
        throw new RuntimeException(
            "invalid app jar path: " + appJar.getAbsolutePath() + ", load failed with exception",
            e);
      }
    }

    URLClassLoader cl = URLClassLoader.newInstance(urls.toArray(new URL[urls.size()]));

    if (!explicitLoad) {
      return cl;
    }
    for (JarFile jar : jars) {
      try {
        Enumeration<JarEntry> e = jar.entries();
        while (e.hasMoreElements()) {
          JarEntry je = e.nextElement();
          if (je.isDirectory() || !je.getName().endsWith(".class")) {
            continue;
          }

          String className = classNameOfJarEntry(je);
          className = className.replace('/', '.');
          try {
            Class.forName(className, true, cl);
          } catch (ClassNotFoundException e1) {
            e1.printStackTrace();
          }
        }
      } finally {
        IOUtils.closeQuietly(jar);
      }
    }
    return cl;
  }

  private static String classNameOfJarEntry(JarEntry je) {
    return je.getName().substring(0, je.getName().length() - ".class".length());
  }

}
