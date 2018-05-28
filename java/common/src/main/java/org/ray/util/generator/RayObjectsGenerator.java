package org.ray.util.generator;

import java.io.IOException;
import org.ray.util.FileUtil;

/**
 * Generate all classes in org.ray.api.returns.RayObjectsX
 */
public class RayObjectsGenerator {

  public static void main(String[] args) throws IOException {
    String rootdir = System.getProperty("user.dir") + "/../api/src/main/java";
    rootdir += "/org/ray/api/returns";
    for (int r = 2; r <= Share.MAX_R; r++) {
      String str = build(r);
      String file = rootdir + "/RayObjects" + r + ".java";
      FileUtil.overrideFile(file, str);
      System.err.println("override " + file);
    }
  }

  /*
   * package org.ray.api.returns;
   *
   * import org.ray.api.RayObject; import org.ray.spi.model.UniqueID;
   *
   * @SuppressWarnings({"rawtypes", "unchecked"}) public class RayObjects2<R0, R1> extends
   * RayObjects {
   *
   * public RayObjects2(UniqueID[] ids) { super(ids); }
   *
   * public RayObjects2(RayObject objs[]) { super(objs); }
   *
   * public RayObject<R0> r0() { return objs[0]; }
   *
   * public RayObject<R1> r1() { return objs[1]; } }
   */
  private static String build(int Rcount) {
    StringBuilder sb = new StringBuilder();
    sb.append("package org.ray.api.returns;\n");
    sb.append("import org.ray.api.*;\n");
    sb.append("import org.ray.spi.model.UniqueID;\n");
    sb.append("@SuppressWarnings({\"rawtypes\", \"unchecked\"})");
    sb.append("public class RayObjects").append(Rcount).append("<")
        .append(Share.buildClassDeclare(0, Rcount)).append("> extends RayObjects {")
        .append("\n");
    sb.append("\tpublic RayObjects").append(Rcount).append("(UniqueID[] ids) {").append("\n");
    sb.append("\t\tsuper(ids);").append("\n");
    sb.append("\t}").append("\n");
    sb.append("\tpublic RayObjects").append(Rcount).append("(RayObject objs[]) {").append("\n");
    sb.append("\t\tsuper(objs);").append("\n");
    sb.append("\t}").append("\n");

    for (int k = 0; k < Rcount; k++) {
      sb.append(buildGetter(k));
    }

    sb.append("}").append("\n");
    return sb.toString();
  }

  /**
   * public RayObject<R0> r0() { return objs[0]; }
   */
  private static String buildGetter(int index) {
    return "\tpublic RayObject<R" + index + "> r" + index + "() {\n"
        + "\t\treturn objs[" + index + "];\n"
        + "\t}\n";
  }

}
