package org.ray.hook;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.List;
import java.util.Scanner;
import java.util.function.BiConsumer;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;
import java.util.zip.DataFormatException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.ray.hook.runtime.JarLoader;
import org.ray.hook.runtime.LoadedFunctions;
import org.ray.util.logger.RayLog;

/**
 * rewrite jars to new jars with methods marked using Ray annotations
 */
public class JarRewriter {

  private static final String FUNCTIONS_FILE = "ray.functions.txt";

  public static void main(String[] args)
      throws IOException, SecurityException, DataFormatException {
    if (args.length == 1) {
      LoadedFunctions funcs = load(args[0], null);
      for (MethodId mi : funcs.functions) {
        System.err.println(mi.getIdMethodDesc());
        Method m = mi.load();
        String logInfo = "load: " + m.getDeclaringClass().getName() + "." + m.getName();
        RayLog.core.info(logInfo);
      }
      return;
    } else if (args.length < 2) {
      System.err.println("org.ray.hook.JarRewriter source-jar-dir dest-jar-dir");
      System.exit(1);
    }

    rewrite(args[0], args[1]);
  }

  public static void rewrite(String fromDir, String toDir) throws IOException, DataFormatException {
    File fromDirFile = new File(fromDir);
    File toDirFileTmp = new File(toDir + ".tmp");
    File toDirFile = new File(toDir);

    File[] topFiles = fromDirFile.listFiles();
    if (topFiles.length != 1 || !topFiles[0].isDirectory()) {
      throw new DataFormatException("There should be a top dir in the Ray app zip file.");
    }
    String topDir = topFiles[0].getName();

    if (toDirFileTmp.exists()) {
      FileUtils.deleteDirectory(toDirFileTmp);
    }
    //toDirFileTmp.mkdir();
    FileUtils.copyDirectory(fromDirFile, toDirFileTmp);

    PrintWriter functionCollector = new PrintWriter(toDir + ".tmp/" + FUNCTIONS_FILE, "UTF-8");

    // get all jars
    Collection<File> files = FileUtils.listFiles(
        fromDirFile,
        new RegexFileFilter(".*\\.jar"),
        DirectoryFileFilter.DIRECTORY
    );

    // load and rewrite
    int prefixLength = fromDirFile.getAbsolutePath().length() + topDir.length() + 2;
    for (File appJar : files) {
      String fromPath = appJar.getAbsolutePath();
      if (fromPath.substring(prefixLength).contains("/")) {
        functionCollector.close();
        throw new DataFormatException("There should not be any subdir"
            + " containing jar file in the top dir of the Ray app zip file.");
      }
      JarFile jar = new JarFile(appJar.getAbsolutePath());
      String to = fromPath
          .replaceFirst(fromDirFile.getAbsolutePath(), toDirFileTmp.getAbsolutePath());
      rewrite(jar, to, (l, m) -> functionCollector.println(m.toEncodingString()));
      jar.close();
    }

    // rename the whole dir
    functionCollector.close();

    if (toDirFile.exists()) {
      FileUtils.deleteDirectory(toDirFile);
    }

    FileUtils.moveDirectory(toDirFileTmp, toDirFile);
  }

  public static LoadedFunctions load(String dir, String baseDir)
      throws FileNotFoundException, SecurityException {
    List<String> functions = JarRewriter.getRewrittenFunctions(dir);
    LoadedFunctions efuncs = new LoadedFunctions();
    efuncs.loader = JarLoader.loadJars(dir, false);

    for (String func : functions) {
      MethodId mid = new MethodId(func, efuncs.loader);
      efuncs.functions.add(mid);
    }

    if (baseDir != null && !baseDir.equals("")) {
      List<String> baseFunctions = JarRewriter.getRewrittenFunctions(baseDir);
      for (String func : baseFunctions) {
        MethodId mid = new MethodId(func, efuncs.loader);
        efuncs.functions.add(mid);
      }
    }

    return efuncs;
  }

  public static LoadedFunctions loadBase(String baseDir)
      throws FileNotFoundException, SecurityException {
    List<String> functions = JarRewriter.getRewrittenFunctions(baseDir);
    LoadedFunctions efuncs = new LoadedFunctions();
    efuncs.loader = null;

    for (String func : functions) {
      MethodId mid = new MethodId(func, efuncs.loader);
      efuncs.functions.add(mid);
    }

    return efuncs;
  }

  public static List<String> getRewrittenFunctions(String rewrittenDir)
      throws FileNotFoundException {
    ArrayList<String> functions = new ArrayList<>();
    Scanner s = new Scanner(new File(rewrittenDir + "/" + FUNCTIONS_FILE));
    while (s.hasNext()) {
      String f = s.next();
      if (!f.startsWith("(")) {
        functions.add(f);
      }
    }
    s.close();

    return functions;
  }

  public static void rewrite(JarFile from, String to, BiConsumer<ClassLoader, MethodId> consumer)
      throws IOException {

    FileOutputStream ofStream = new FileOutputStream(to);
    JarOutputStream ojStream = new JarOutputStream(ofStream);
    Enumeration<JarEntry> e = from.entries();
    String className;

    while (e.hasMoreElements()) {
      JarEntry je = e.nextElement();
      byte[] jeBytes = IOUtils.toByteArray(from.getInputStream(je));

      //System.err.println("XXXXXX " + from.getName() + " :: " + je.getName());
      if (!je.isDirectory() && je.getName().endsWith(".class")) {
        className = je.getName().substring(0, je.getName().length() - ".class".length());

        //System.err.println("XXXXXX " + from.getName() + " :: " + je.getName() + " - " + className);
        ClassAdapter.Result result = ClassAdapter.hookClass(null, className, jeBytes);
        if (result.classBuffer != jeBytes) {
          String logInfo = "Rewrite class " + className + " from " + jeBytes.length + " bytes to "
              + result.classBuffer.length + " bytes ";
          RayLog.core.info(logInfo);
        }

        if (result.changedMethods != null) {
          for (MethodId m : result.changedMethods) {
            consumer.accept(null, m);
          }
        }

        je = new JarEntry(je.getName());
        je.setTime(System.currentTimeMillis());
        je.setSize(result.classBuffer.length);
        jeBytes = result.classBuffer;
      }

      ojStream.putNextEntry(je);
      ojStream.write(jeBytes);
      //ojStream.closeEntry();
    }

    ojStream.close();
    ofStream.close();
  }
}
