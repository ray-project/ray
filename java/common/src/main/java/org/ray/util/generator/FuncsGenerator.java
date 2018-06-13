package org.ray.util.generator;

import java.io.IOException;
import org.ray.util.FileUtil;
import org.ray.util.generator.Composition.Tr;

/**
 * Generate all classes in org.ray.api.funcs
 */
public class FuncsGenerator {

  public static void main(String[] args) throws IOException {
    String rootdir = System.getProperty("user.dir") + "/../api/src/main/java";
    rootdir += "/org/ray/api/funcs";
    generate(rootdir);
  }

  private static void generate(String rootdir) throws IOException {
    for (Tr tr : Composition.calculate(Share.MAX_T, Share.MAX_R)) {
      String str = build(tr.tcount, tr.rcount);
      String file = rootdir + "/RayFunc_" + tr.tcount + "_"
          + (tr.rcount <= 0 ? (tr.rcount == 0 ? "n" : "n_list") : tr.rcount) + ".java";
      FileUtil.overrideFile(file, str);
      System.err.println("override " + file);
    }
  }

  /*
   * package org.ray.api.funcs;
   *
   * @FunctionalInterface
   * public interface RayFunc_4_1<T0, T1, T2, T3, R0> extends RayFunc { R0
   * apply();
   *
   * public static <R0> R0 execute(Object[] args) throws Throwable { String name =
   * (String)args[args.length - 2]; assert (name.equals(RayFunc_0_1.class.getName())); byte[]
   * funcBytes = (byte[])args[args.length - 1]; RayFunc_0_1<R0> f = (RayFunc_0_1<R0>)
   * SerializationUtils.deserialize(funcBytes);
   * return f.apply(); } }
   */
  private static String build(int tcount, int rcount) {
    StringBuilder sb = new StringBuilder();
    String tname =
        "Ray" + "Func_" + tcount + "_" + (rcount <= 0 ? (rcount == 0 ? "n" : "n_list") : rcount);
    final String gname = tname + "<" + Share.buildClassDeclare(tcount, rcount) + ">";

    sb.append("package org.ray.api.funcs;").append("\n");
    if (rcount > 1) {
      sb.append("import org.ray.api.returns.*;").append("\n");
    }
    if (rcount <= 0) {
      sb.append("import java.util.Collection;").append("\n");
      sb.append("import java.util.List;").append("\n");
      sb.append("import java.util.Map;").append("\n");
    }
    sb.append("import org.ray.api.*;").append("\n");
    sb.append("import org.ray.api.internal.*;").append("\n");
    sb.append("import org.apache.commons.lang3.SerializationUtils;").append("\n");
    sb.append("\n");

    sb.append("@FunctionalInterface").append("\n");
    sb.append("public interface ").append(gname).append(" extends RayFunc {")
        .append("\n");
    sb.append("\t").append(Share.buildFuncReturn(rcount)).append(" apply(")
        .append(rcount == 0 ? ("Collection<RID> returnids" + (tcount > 0 ? ", " : "")) : "")
        .append(Share.buildParameter(tcount, "T", null)).append(") throws Throwable;")
        .append("\n");

    sb.append("\t\n");
    sb.append("\tpublic static " + "<").append(Share.buildClassDeclare(tcount, rcount))
        .append(">").append(" ").append(Share.buildFuncReturn(rcount))
        .append(" execute(Object[] args) throws Throwable {").append("\n");
    sb.append("\t\tString name = (String)args[args.length - 2];").append("\n");
    sb.append("\t\tassert (name.equals(").append(tname).append(".class.getName()));").append("\n");
    sb.append("\t\tbyte[] funcBytes = (byte[])args[args.length - 1];").append("\n");
    sb.append("\t\t").append(gname).append(" f = SerializationUtils.deserialize(funcBytes);")
        .append("\n");
    sb.append("\t\treturn f.apply(")
        .append(rcount == 0 ? ("(Collection<RID>)args[0]" + (tcount > 0 ? ", " : "")) : "")
        .append(Share.buildParameterUse2(tcount, rcount == 0 ? 1 : 0, "T", "args[", "]"))
        .append(");").append("\n");
    sb.append("\t}").append("\n");
    sb.append("\t\n");
    sb.append("}").append("\n");
    return sb.toString();
  }

}
