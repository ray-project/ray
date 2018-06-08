package org.ray.core;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.ray.util.MethodId;
import org.ray.util.logger.RayLog;

/**
 * load and unload jars from a dir
 */
public class JarLoader {

  private static Method AddUrl = InitAddUrl();

  private static Method InitAddUrl() {
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

  public static URLClassLoader loadJars(String dir, boolean explicitLoad, Set<MethodId> methods) {
    // get all jars
    Collection<File> jars = FileUtils.listFiles(
        new File(dir),
        new RegexFileFilter(".*\\.jar"),
        DirectoryFileFilter.DIRECTORY
    );
    return loadJar(jars, explicitLoad, methods);
  }

  public static ClassLoader loadJars(String[] appJars, boolean explicitLoad,
      Set<MethodId> methods) {
    List<File> jars = new ArrayList<>();

    for (String jar : appJars) {
      if (jar.endsWith(".jar")) {
        jars.add(new File(jar));
      } else {
        loadJarDir(jar, jars);
      }
    }

    return loadJar(jars, explicitLoad, methods);
  }

  public static void unloadJars(ClassLoader loader) {
    // TODO:
  }

  private static URLClassLoader loadJar(Collection<File> appJars, boolean explicitLoad,
      Set<MethodId> methods) {
    List<JarFile> jars = new ArrayList<>();
    List<URL> urls = new ArrayList<>();

    for (File appJar : appJars) {
      try {
        RayLog.core.info("load jar " + appJar.getAbsolutePath());
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

    URLClassLoader cl = URLClassLoader.newInstance(urls.toArray(new URL[urls.size()]));

    for (JarFile jar : jars) {
      try {
        Enumeration<JarEntry> e = jar.entries();
        while (e.hasMoreElements()) {
          JarEntry je = e.nextElement();
          //System.err.println("check " + je.getName());
          if (je.isDirectory() || !je.getName().endsWith(".class")) {
            continue;
          }

          String className = classNameOfJarEntry(je);
          className = className.replace('/', '.');
          //to scan @RayRemote
          try {
            scanRayRemoteAnnotation(jar, methods);
          } catch (Exception e1) {
            e1.printStackTrace();
            System.err.println(
                "scanRayRemoteAnnotation class: " + className + ", scan failed with exception "
                    + e1.getMessage());
            System.exit(1);
          }
          if (explicitLoad) {
            try {
              Class.forName(className, true, cl);
              //System.err.println("load class " + className + " OK");
            } catch (ClassNotFoundException e1) {
              e1.printStackTrace();
            }
          }
        }
      } finally {
        IOUtils.closeQuietly(jar);
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

  private static String classNameOfJarEntry(JarEntry je) {
    return je.getName().substring(0, je.getName().length() - ".class".length());
  }

  public static void scanRayRemoteAnnotation(JarFile jarFile, final Set<MethodId> methods)
      throws Exception {
    Enumeration<JarEntry> e = jarFile.entries();

    while (e.hasMoreElements()) {
      JarEntry je = e.nextElement();
      if (je.isDirectory() || !je.getName().endsWith(".class")) {
        continue;
      }
      final String className = classNameOfJarEntry(je);
      final InputStream jeInputStream = jarFile.getInputStream(je);
      try {
        byte[] jeBytes = IOUtils.toByteArray(jeInputStream);
        ClassReader reader = new ClassReader(jeBytes);
        final AtomicBoolean isActor = new AtomicBoolean();
        reader.accept(new ClassVisitor(Opcodes.ASM6) {
          @Override
          public AnnotationVisitor visitAnnotation(String desc, boolean visible) {
            if (desc.contains("Lorg/ray/api/RayRemote;")) {
              isActor.set(true);
              RayLog.core.info("scan Actor:" + className);
            }
            return super.visitAnnotation(desc, visible);
          }

          @Override
          public MethodVisitor visitMethod(int access, String name, String mdesc, String signature,
              String[] exceptions) {
            if (name.equals("<init>")) {
              return super.visitMethod(access, name, mdesc, signature, exceptions);
            }
            if (isActor.get() && (access & Opcodes.ACC_PUBLIC) != 0) {
              MethodId m = new MethodId(className, name, mdesc, (access & Opcodes.ACC_STATIC) != 0);
              methods.add(m);
              RayLog.core.info("scan Actor's method:" + m);
            }

            MethodVisitor origin = super.visitMethod(access, name, mdesc, signature, exceptions);
            return new MethodVisitor(this.api, origin) {
              @Override
              public AnnotationVisitor visitAnnotation(String adesc, boolean visible) {
                if (adesc.contains("Lorg/ray/api/RayRemote;")) {
                  MethodId m = new MethodId(className, name, mdesc,
                      (access & Opcodes.ACC_STATIC) != 0);
                  methods.add(m);
                  RayLog.core.info("scan Task's method:" + m);
                }
                return super.visitAnnotation(adesc, visible);
              }
            };
          }
        }, ClassReader.SKIP_FRAMES);
      } finally {
        IOUtils.closeQuietly(jeInputStream);
      }
    }
  }
}
