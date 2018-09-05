package org.ray.util.generator;

import java.io.IOException;
import org.ray.util.FileUtil;

/**
 * A util class that generates all the RayFuncX classes under org.ray.api.function package.
 */
public class RayFuncGenerator extends BaseGenerator {

  private String generate(int numParameters) {
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
    newLine(String.format("public interface RayFunc%d<%sR> extends RayFunc {",
        numParameters, genericTypes));
    indents(1);
    newLine(String.format("R apply(%s);", paramList));
    newLine("}");

    return sb.toString();
  }

  public static void main(String[] args) throws IOException {
    String root = System.getProperty("user.dir")
        + "/api/src/main/java/org/ray/api/function/";
    RayFuncGenerator generator = new RayFuncGenerator();
    for (int i = 0; i <= MAX_PARAMETERS; i++) {
      String content = generator.generate(i);
      FileUtil.overrideFile(root + "RayFunc" + i + ".java", content);
    }
  }

}
