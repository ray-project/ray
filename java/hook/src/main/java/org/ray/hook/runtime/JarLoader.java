package org.ray.hook.runtime;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.ray.util.logger.RayLog;

/**
 * load and unload jars from a dir.
 */
public class JarLoader {

  private static Method AddUrl = initAddUrl();

  private static Method initAddUrl() {
    try {
      Method m = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
      m.setAccessible(true);
      return m;
    } catch (NoSuchMethodException | SecurityException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      return null;
    }
  }

  public static URLClassLoader loadJars(String dir, boolean explicitLoadForHook) {
    // get all jars
    Collection<File> jars = FileUtils.listFiles(
        new File(dir),
        new RegexFileFilter(".*\\.jar"),
        DirectoryFileFilter.DIRECTORY
    );
    return loadJar(jars, explicitLoadForHook);
  }

  public static ClassLoader loadJars(String[] appJars, boolean explicitLoadForHook) {
    List<File> jars = new ArrayList<>();

    for (String jar : appJars) {
      if (jar.endsWith(".jar")) {
        jars.add(new File(jar));
      } else {
        loadJarDir(jar, jars);
      }
    }

    return loadJar(jars, explicitLoadForHook);
  }

  private static URLClassLoader loadJar(Collection<File> appJars, boolean explicitLoadForHook) {
    List<JarFile> jars = new ArrayList<>();
    List<URL> urls = new ArrayList<>();

    for (File appJar : appJars) {
      try {
        RayLog.core.info("load jar " + appJar.getAbsolutePath() + " in ray hook");
        JarFile jar = new JarFile(appJar.getAbsolutePath());
        jars.add(jar);
        urls.add(appJar.toURI().toURL());
      } catch (IOException e) {
        e.printStackTrace();
        System.err.println(
            "invalid app jar path: " + appJar.getAbsolutePath() + ", load failed with exception "
                + e.getMessage());
        System.exit(1);
        return null;
      }
    }

    URLClassLoader cl = URLClassLoader.newInstance(urls.toArray(new URL[0]));

    if (!explicitLoadForHook) {
      return cl;
    }

    for (JarFile jar : jars) {
      Enumeration<JarEntry> e = jar.entries();
      while (e.hasMoreElements()) {
        JarEntry je = e.nextElement();
        //System.err.println("check " + je.getName());
        if (je.isDirectory() || !je.getName().endsWith(".class")) {
          continue;
        }

        // -6 because of .class
        String className = je.getName().substring(0, je.getName().length() - 6);
        className = className.replace('/', '.');
        try {
          Class.forName(className, true, cl);
          //System.err.println("load class " + className + " OK");
        } catch (ClassNotFoundException e1) {
          e1.printStackTrace();
        }
      }

      try {
        jar.close();
      } catch (IOException e1) {
        // TODO Auto-generated catch block
        e1.printStackTrace();
      }
    }
    return cl;
  }

  private static void loadJarDir(String jarDir, List<File> jars) {
    Collection<File> files = FileUtils.listFiles(
        new File(jarDir),
        new RegexFileFilter(".*\\.jar"),
        DirectoryFileFilter.DIRECTORY
    );

    jars.addAll(files);
  }

  public static void unloadJars(ClassLoader loader) {
    // TODO:
  }
}
