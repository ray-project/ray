package io.ray.serialization.encoder;

import com.google.common.base.Preconditions;
import io.ray.serialization.Fury;
import io.ray.serialization.codegen.CodeGenerator;
import io.ray.serialization.codegen.CompileUnit;
import io.ray.serialization.util.LoggerFactory;
import org.slf4j.Logger;

public class CodecUtils {
  private static final Logger LOG = LoggerFactory.getLogger(CodecUtils.class);
  // use this package when bean class name starts with java.
  private static final String FALLBACK_PACKAGE = Generated.class.getPackage().getName();

  public static Class<?> loadOrGenSeqCodecClass(Class<?> beanClass, Fury fury) {
    Preconditions.checkNotNull(fury);
    LOG.debug("Create SeqCodec for class {}", beanClass);
    SeqCodecBuilder codecBuilder = new SeqCodecBuilder(beanClass, fury);
    // use genCodeFunc to avoid gen code repeatedly
    CompileUnit compileUnit =
        new CompileUnit(
            getPackage(beanClass), codecBuilder.codecClassName(beanClass), codecBuilder::genCode);
    CodeGenerator codeGenerator = CodeGenerator.getSharedCodeGenerator(beanClass.getClassLoader());
    ClassLoader classLoader = codeGenerator.compile(compileUnit);
    String className = codecBuilder.codecQualifiedClassName(beanClass);
    try {
      return classLoader.loadClass(className);
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException("Impossible because we just compiled class", e);
    }
  }

  /**
   * Can't create a codec class that has package starts with java, which throws
   * java.lang.SecurityException: Prohibited package name.</br> Caution: The runtime package is
   * defined by package name and classloader. see {@link AccessorHelper}
   */
  static String getPackage(Class<?> cls) {
    String pkg;
    // Janino generated class'package might be null
    if (cls.getPackage() == null) {
      String className = cls.getName();
      int index = className.lastIndexOf(".");
      if (index != -1) {
        pkg = className.substring(0, index);
      } else {
        pkg = "";
      }
    } else {
      pkg = cls.getPackage().getName();
    }
    if (pkg.startsWith("java")) {
      return FALLBACK_PACKAGE;
    } else {
      return pkg;
    }
  }
}
