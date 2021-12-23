package io.ray.serialization.codegen;

import com.google.common.base.Preconditions;
import io.ray.serialization.util.LoggerFactory;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.security.ProtectionDomain;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.codehaus.janino.ByteArrayClassLoader;
import org.slf4j.Logger;

public class CodeGenerator {
  private static final Logger LOG = LoggerFactory.getLogger(CodeGenerator.class);

  // See javassist.ClassPool.
  private static java.lang.reflect.Method defineClass;

  static {
    try {
      AccessController.doPrivileged(
          new PrivilegedExceptionAction<Object>() {
            public Object run() throws Exception {
              Class<?> cl = Class.forName("java.lang.ClassLoader");
              defineClass =
                  cl.getDeclaredMethod(
                      "defineClass",
                      String.class,
                      byte[].class,
                      int.class,
                      int.class,
                      ProtectionDomain.class);
              defineClass.setAccessible(true);
              return null;
            }
          });
    } catch (PrivilegedActionException pae) {
      throw new RuntimeException("cannot initialize CodeGenerator", pae.getException());
    }
  }

  private static final String CODE_DIR_KEY = "FURY_CODE_DIR";
  private static final String DEFAULT_CODE_DIR = ".code";
  private static final String DELETE_CODE_ON_EXIT_KEY = "FURY_DELETE_CODE_ON_EXIT";
  private static final boolean DEFAULT_DELETE_CODE_ON_EXIT = true;

  // This is the default value of HugeMethodLimit in the OpenJDK HotSpot JVM,
  // beyond which methods will be rejected from JIT compilation
  static final int DEFAULT_JVM_HUGE_METHOD_LIMIT = 8000;

  // The max valid length of method parameters in JVM.
  static final int MAX_JVM_METHOD_PARAMS_LENGTH = 255;

  private static WeakHashMap<ClassLoader, CodeGenerator> sharedCodeGenerator = new WeakHashMap<>();

  private ClassLoader classLoader;
  private final ConcurrentHashMap<String, Object> parallelCompileLock;
  private final ConcurrentHashMap<String, Boolean> parallelDefineStatusLock;

  public CodeGenerator(ClassLoader classLoader) {
    Preconditions.checkNotNull(classLoader);
    this.classLoader = classLoader;
    parallelCompileLock = new ConcurrentHashMap<>();
    parallelDefineStatusLock = new ConcurrentHashMap<>();
  }

  /**
   * Compile code, return as a new classloader. If the class of a compile unit already exists in
   * previous classloader, skip the corresponding compile unit.
   *
   * @param compileUnits compile units
   */
  public ClassLoader compile(CompileUnit... compileUnits) {
    List<CompileUnit> toCompile = new ArrayList<>();
    for (CompileUnit unit : compileUnits) {
      if (!classExists(classLoader, fullClassName(unit))) {
        toCompile.add(unit);
      }
    }
    if (!toCompile.isEmpty()) {
      synchronized (getCompileLock(toCompile)) {
        classLoader = doCompile(classLoader, toCompile);
      }
    }
    return classLoader;
  }

  private Object getCompileLock(List<CompileUnit> toCompile) {
    String lockName = "MULTI_COMPILE_UNIT_LOCK";
    if (toCompile.size() == 1) {
      lockName = fullClassName(toCompile.get(0));
    }
    return getCompileLock(lockName);
  }

  private Object getCompileLock(String lockName) {
    return parallelCompileLock.computeIfAbsent(lockName, k -> new Object());
  }

  private Boolean getDefineLock(String className) {
    return parallelDefineStatusLock.computeIfAbsent(className, k -> false);
  }

  private boolean classExists(ClassLoader loader, String className) {
    try {
      loader.loadClass(className);
      return true;
    } catch (ClassNotFoundException e) {
      return false;
    }
  }

  /**
   * Compile code, return as a new classloader if can't define class in passed classloader, use
   * parentClassLoader to find other classes that compileUnits needs . If the class of a compile
   * unit already exists in previous classloader, don't skip the corresponding compile unit, compile
   * and define class in returned classloader.
   *
   * @param parentClassLoader parent classLoader to find other classes that compileUnits needs
   * @param compileUnits compile units
   */
  private ClassLoader doCompile(ClassLoader parentClassLoader, List<CompileUnit> compileUnits) {
    Preconditions.checkArgument(!compileUnits.isEmpty());
    long startTime = System.nanoTime();
    compileUnits =
        compileUnits.stream()
            .filter(unit -> !classExists(parentClassLoader, fullClassName(unit)))
            .collect(Collectors.toList());
    if (compileUnits.isEmpty()) {
      return parentClassLoader;
    }
    Map<String, byte[]> classesByteCodes =
        JaninoUtils.toBytecode(parentClassLoader, compileUnits.toArray(new CompileUnit[0]));
    long durationMs = (System.nanoTime() - startTime) / 1000_000;
    if (LOG.isInfoEnabled()) {
      String classes =
          compileUnits.stream()
              .map(unit -> unit.mainClassName)
              .collect(Collectors.joining(", ", "[", "]"));
      LOG.info("Compile {} take {} ms", classes, durationMs);
    }
    Map<String, byte[]> classes = new HashMap<>(classesByteCodes);
    if (parentClassLoader instanceof ByteArrayClassLoader) {
      for (Map.Entry<String, byte[]> entry : classesByteCodes.entrySet()) {
        String className = fullClassNameFromClassFilePath(entry.getKey());
        // Avoid multi-compile unit classes define operation collision with single compile unit.
        if (!getDefineLock(className)) { // class not defined yet.
          synchronized (getDefineLock(className)) {
            if (!getDefineLock(className)) { // class not defined yet.
              // Even if multiple compile unit is inter-dependent, they can still be defined
              // separately.
              if (tryDefineClassesInClassLoader(parentClassLoader, className, entry.getValue())) {
                classes.remove(entry.getKey());
              }
            }
            parallelDefineStatusLock.put(className, true);
          }
        }
      }
    }
    if (classes.isEmpty()) {
      return parentClassLoader;
    } else {
      // Set up a class loader that finds and defined the generated classes.
      return new ByteArrayClassLoader(classes, parentClassLoader);
    }
  }

  public static synchronized CodeGenerator getSharedCodeGenerator(ClassLoader classLoader) {
    if (classLoader == null) {
      classLoader = CodeGenerator.class.getClassLoader();
    }
    return sharedCodeGenerator.computeIfAbsent(classLoader, CodeGenerator::new);
  }

  public static boolean tryDefineClassesInClassLoader(
      ClassLoader classLoader, String className, byte[] bytecode) {
    try {
      defineClass.invoke(
          classLoader,
          className,
          bytecode,
          0,
          bytecode.length,
          classLoader.getClass().getProtectionDomain());
    } catch (Exception e) {
      LOG.debug("Unable define class {} in classloader {}.", className, classLoader, e);
      return false;
    }
    return true;
  }

  static String getCodeDir() {
    String codeDir = System.getProperty(CODE_DIR_KEY, System.getenv(CODE_DIR_KEY));
    if (codeDir == null) {
      codeDir = DEFAULT_CODE_DIR;
    }
    return codeDir;
  }

  static boolean deleteCodeOnExit() {
    boolean deleteCodeOnExit = DEFAULT_DELETE_CODE_ON_EXIT;
    String deleteCodeOnExitStr =
        System.getProperty(DELETE_CODE_ON_EXIT_KEY, System.getenv(DELETE_CODE_ON_EXIT_KEY));
    if (deleteCodeOnExitStr != null) {
      deleteCodeOnExit = Boolean.parseBoolean(deleteCodeOnExitStr);
    }
    return deleteCodeOnExit;
  }

  public static String classFilepath(CompileUnit unit) {
    return classFilepath(fullClassName(unit));
  }

  public static String classFilepath(String pkg, String className) {
    return classFilepath(pkg + "." + className);
  }

  public static String classFilepath(String fullClassName) {
    int index = fullClassName.lastIndexOf(".");
    if (index >= 0) {
      return String.format(
          "%s/%s.class",
          fullClassName.substring(0, index).replace(".", "/"), fullClassName.substring(index + 1));
    } else {
      return fullClassName + ".class";
    }
  }

  public static String fullClassName(CompileUnit unit) {
    return unit.pkg + "." + unit.mainClassName;
  }

  public static String fullClassNameFromClassFilePath(String classFilePath) {
    return classFilePath.substring(0, classFilePath.length() - ".class".length()).replace("/", ".");
  }

  /** align code to have 4 spaces indent */
  static String alignIndent(String code) {
    return alignIndent(code, 4);
  }

  /** align code to have {@code numSpaces} spaces indent */
  static String alignIndent(String code, int numSpaces) {
    if (code == null) {
      return "";
    }
    String[] split = code.split("\n");
    if (split.length == 1) {
      return code;
    } else {
      StringBuilder codeBuilder = new StringBuilder(split[0]).append('\n');
      for (int i = 1; i < split.length; i++) {
        for (int j = 0; j < numSpaces; j++) {
          codeBuilder.append(' ');
        }
        codeBuilder.append(split[i]).append('\n');
      }
      if (code.charAt(code.length() - 1) == '\n') {
        return codeBuilder.toString();
      } else {
        return codeBuilder.substring(0, codeBuilder.length() - 1);
      }
    }
  }

  /** indent code by 4 spaces */
  static String indent(String code) {
    return indent(code, 4);
  }

  /** The implementation shouldn't add redundant newline separator */
  static String indent(String code, int numSpaces) {
    if (code == null) {
      return "";
    }
    String[] split = code.split("\n");
    StringBuilder codeBuilder = new StringBuilder();
    for (String line : split) {
      for (int i = 0; i < numSpaces; i++) {
        codeBuilder.append(' ');
      }
      codeBuilder.append(line).append('\n');
    }
    if (code.charAt(code.length() - 1) == '\n') {
      return codeBuilder.toString();
    } else {
      return codeBuilder.substring(0, codeBuilder.length() - 1);
    }
  }

  /**
   * @param numSpaces spaces num
   * @return a string of numSpaces spaces
   */
  static String spaces(int numSpaces) {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < numSpaces; i++) {
      builder.append(' ');
    }
    return builder.toString();
  }

  static void appendNewlineIfNeeded(StringBuilder sb) {
    if (sb.length() > 0 && sb.charAt(sb.length() - 1) != '\n') {
      sb.append('\n');
    }
  }

  static StringBuilder stripLastNewline(StringBuilder sb) {
    int length = sb.length();
    Preconditions.checkArgument(length > 0 && sb.charAt(length - 1) == '\n');
    sb.deleteCharAt(length - 1);
    return sb;
  }

  static StringBuilder stripIfHasLastNewline(StringBuilder sb) {
    int length = sb.length();
    if (length > 0 && sb.charAt(length - 1) == '\n') {
      sb.deleteCharAt(length - 1);
    }
    return sb;
  }
}
