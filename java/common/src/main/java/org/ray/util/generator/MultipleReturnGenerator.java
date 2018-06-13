package org.ray.util.generator;

import java.io.IOException;
import org.ray.util.FileUtil;

/**
 * Generate all classes in org.ray.api.returns.MultipleReturnsX
 */
public class MultipleReturnGenerator {

  public static void main(String[] args) throws IOException {
    String rootdir = System.getProperty("user.dir") + "/../api/src/main/java";
    rootdir += "/org/ray/api/returns";
    for (int r = 2; r <= Share.MAX_R; r++) {
      String str = build(r);
      String file = rootdir + "/MultipleReturns" + r + ".java";
      FileUtil.overrideFile(file, str);
      System.err.println("override " + file);
    }
  }

  /**
   * package org.ray.api.returns.
   */
  private static String build(int rcount) {
    StringBuilder sb = new StringBuilder();
    sb.append("package org.ray.api.returns;").append("\n");
    sb.append("import org.ray.api.*;").append("\n");
    sb.append("@SuppressWarnings(\"unchecked\")");
    sb.append("public class MultipleReturns").append(rcount).append("<")
        .append(Share.buildClassDeclare(0, rcount)).append("> extends MultipleReturns {")
        .append("\n");
    sb.append("\tpublic MultipleReturns").append(rcount).append("(")
        .append(Share.buildParameter(rcount, "R", null)).append(") {")
        .append("\n");
    sb.append("\t\tsuper(new Object[] { ").append(Share.buildParameterUse(rcount, "R"))
        .append(" });")
        .append("\n");
    sb.append("\t}").append("\n");

    for (int k = 0; k < rcount; k++) {
      sb.append(buildGetter(k));
    }

    sb.append("}").append("\n");
    return sb.toString();
  }

  /*
   * @SuppressWarnings("unchecked") public R1 get1() { return (R1) this.values[1]; }
   */
  private static String buildGetter(int index) {
    return ("\tpublic R" + index + " get" + index + "() {\n")
        + "\t\treturn (R" + index + ") this.values[" + index + "];\n"
        + "\t}\n";
  }

}
