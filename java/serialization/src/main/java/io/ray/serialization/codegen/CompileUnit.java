package io.ray.serialization.codegen;

import com.google.common.base.Preconditions;
import io.ray.serialization.util.LoggerFactory;
import java.util.function.Supplier;
import org.slf4j.Logger;

/** A CompileUnit corresponds to java file, which have a package, main class and code */
public class CompileUnit {
  private static final Logger LOG = LoggerFactory.getLogger(CompileUnit.class);

  String pkg;
  String mainClassName;
  private String code;
  private Supplier<String> genCodeFunc;

  public CompileUnit(String pkg, String mainClassName, String code) {
    this.pkg = pkg;
    this.mainClassName = mainClassName;
    this.code = code;
  }

  public CompileUnit(String pkg, String mainClassName, Supplier<String> genCodeFunc) {
    this.pkg = pkg;
    this.mainClassName = mainClassName;
    this.genCodeFunc = genCodeFunc;
  }

  public String getCode() {
    if (code == null) {
      Preconditions.checkNotNull(genCodeFunc);
      long startTime = System.nanoTime();
      code = genCodeFunc.get();
      long durationMs = (System.nanoTime() - startTime) / 1000_000;
      LOG.info("Generate code for {}.{} took {} ms.", pkg, mainClassName, durationMs);
    }
    return code;
  }

  @Override
  public String toString() {
    return "CompileUnit{" + "pkg='" + pkg + '\'' + ", mainClassName='" + mainClassName + '\'' + '}';
  }
}
