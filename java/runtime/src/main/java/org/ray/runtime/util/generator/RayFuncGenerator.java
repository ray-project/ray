package org.ray.runtime.util.generator;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import org.apache.commons.io.FileUtils;

/**
 * A util class that generates all the RayFuncX classes under org.ray.api.function package.
 */
public class RayFuncGenerator extends BaseGenerator {

  private String generate(int numParameters, boolean hasReturn) {
    sb = new StringBuilder();

    String genericTypes = "";
    String paramList = "";
    for (int i = 0; i < numParameters; i++) {
      genericTypes += "T" + i + ", ";
      if (i > 0) {
        paramList += ", ";
      }
      paramList += String.format("T%d t%d", i, i);
    }
    if (hasReturn) {
      genericTypes += "R, ";
    }
    if (!genericTypes.isEmpty()) {
      // Remove trailing ", ".
      genericTypes = genericTypes.substring(0, genericTypes.length() - 2);
      genericTypes = "<" + genericTypes + ">";
    }

    newLine("// generated automatically, do not modify.");
    newLine("");
    newLine("package org.ray.api.function;");
    newLine("");
    newLine("/**");
    String comment = String.format(
        " * Functional interface for a remote function that has %d parameter%s.",
        numParameters, numParameters > 1 ? "s" : "");
    newLine(comment);
    newLine(" */");
    newLine("@FunctionalInterface");
    String className = "RayFunc" + (hasReturn ? "" : "Void") + numParameters;
    newLine(String.format("public interface %s%s extends %s {",
        className, genericTypes, hasReturn ? "RayFunc" : "RayFuncVoid"));
    newLine("");
    indents(1);
    newLine(String.format("%s apply(%s) throws Exception;", hasReturn ? "R" : "void", paramList));
    newLine("}");

    return sb.toString();
  }

  public static void main(String[] args) throws IOException {
    String root = System.getProperty("user.dir")
        + "/api/src/main/java/org/ray/api/function/";
    RayFuncGenerator generator = new RayFuncGenerator();
    for (int i = 0; i <= MAX_PARAMETERS; i++) {
      // Functions that have return.
      String content = generator.generate(i, true);
      FileUtils.write(new File(root + "RayFunc" + i + ".java"), content,
          Charset.defaultCharset());
      // Functions that don't have return.
      content = generator.generate(i, false);
      FileUtils.write(new File(root + "RayFuncVoid" + i + ".java"), content,
          Charset.defaultCharset());
    }
  }

}
