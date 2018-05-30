package org.ray.util.generator;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.ray.util.FileUtil;
import org.ray.util.generator.Composition.TR;

/**
 * Generate Rpc.java
 */
public class RpcGenerator {

  public static void main(String[] args) throws IOException {
    String rootdir = System.getProperty("user.dir") + "/../api/src/main/java";
    rootdir += "/org/ray/api/Rpc.java";
    FileUtil.overrideFile(rootdir, build());
  }

  private static String build() {
    StringBuilder sb = new StringBuilder();

    sb.append("package org.ray.api.internal;\n");
    sb.append("import org.ray.api.funcs.*;\n");
    sb.append("import org.ray.api.returns.*;\n");
    sb.append("import org.ray.api.*;\n");
    sb.append("import java.util.Collection;\n");
    sb.append("import java.util.Map;\n");

    sb.append("@SuppressWarnings({\"rawtypes\", \"unchecked\"})\n");
    sb.append("class Rpc {\n");
    for (TR tr : Composition.calculate(Share.MAX_T, Share.MAX_R)) {
      buildCall(sb, tr.Tcount, tr.Rcount);
    }
    sb.append("}\n");
    return sb.toString();
  }

  private static void buildCall(StringBuilder sb, int Tcount, int Rcount) {
    for (Set<Integer> whichTisFuture : whichTisFutureComposition(Tcount)) {
      sb.append(buildCall(Tcount, Rcount, whichTisFuture));
    }
  }

  /**
   * public static <T0, R0> RayObject<R0> call(RayFunc_1_1<T0, R0> f, RayObject<T0> arg) { return
   * Ray.runtime().rpc(() -> f.apply(null), arg).objs[0]; }
   */
  private static String buildCall(int Tcount, int Rcount, Set<Integer> whichTisFuture) {
    StringBuilder sb = new StringBuilder();
    String parameter = (Tcount == 0 ? ""
        : ", " + Share.buildParameter(Tcount, "T", whichTisFuture));
    sb.append("\tpublic static <").append(Share.buildClassDeclare(Tcount, Rcount)).append("> ")
        .append(Share.buildRpcReturn(Rcount)).append(" call")
        .append(Rcount == 1 ? "" : (Rcount <= 0 ? "_n" : ("_" + Rcount))).append("(RayFunc_")
        .append(Tcount).append("_")
        .append(Rcount <= 0 ? (Rcount == 0 ? "n" : "n_list") : Rcount).append("<")
        .append(Share.buildClassDeclare(Tcount, Rcount)).append("> f").append(
        Rcount <= 0 ? (Rcount == 0 ? ", Collection<RID> returnids" : ", Integer returnCount")
            : "").append(parameter).append(") {\n");
        
        /*
         * public static <R0> RayObject<R0> call(RayFunc_0_1<R0> f) {
                if (Ray.Parameters().remoteLambda()) {
                    return Ray.internal().call(RayFunc_0_1.class, f, 1).objs[0];
                }
                else {
                    return Ray.internal().call(() -> f.apply(), 1).objs[0];
                }
            }
         */

    String nulls = Share.buildRepeat("null",
        Tcount + (Rcount == 0 ? 1/*for first arg map*/ : 0));
    String parameterUse = (Tcount == 0 ? "" : (", " + Share.buildParameterUse(Tcount, "T")));
    String labmdaUse = "RayFunc_"
        + Tcount + "_" + (Rcount <= 0 ? (Rcount == 0 ? "n" : "n_list") : Rcount)
        + ".class, f";

    sb.append("\t\tif (Ray.Parameters().remoteLambda()) {\n");
    if (Rcount == 1) {
      sb.append("\t\t\treturn Ray.internal().call(null, ").append(labmdaUse).append(", 1")
          .append(parameterUse).append(").objs[0];")
          .append("\n");
    } else if (Rcount == 0) {
      sb.append("\t\t\treturn Ray.internal().callWithReturnLabels(null, ")
          .append(labmdaUse).append(", returnids").append(parameterUse).append(");")
          .append("\n");
    } else if (Rcount < 0) {
      sb.append("\t\t\treturn Ray.internal().callWithReturnIndices(null, ")
          .append(labmdaUse).append(", returnCount").append(parameterUse).append(");")
          .append("\n");
    } else {
      sb.append("\t\t\treturn new RayObjects").append(Rcount)
          .append("(Ray.internal().call(null, ").append(labmdaUse).append(", ").append(Rcount)
          .append(parameterUse).append(").objs);")
          .append("\n");
    }
    sb.append("\t\t} else {\n");
    if (Rcount == 1) {
      sb.append("\t\t\treturn Ray.internal().call(null, () -> f.apply(").append(nulls)
          .append("), 1").append(parameterUse).append(").objs[0];")
          .append("\n");
    } else if (Rcount == 0) {
      sb.append("\t\t\treturn Ray.internal().callWithReturnLabels(null, () -> f.apply(")
          .append(nulls).append("), returnids").append(parameterUse).append(");")
          .append("\n");
    } else if (Rcount < 0) {
      sb.append("\t\t\treturn Ray.internal().callWithReturnIndices(null, () -> f.apply(")
          .append(nulls).append("), returnCount").append(parameterUse).append(");")
          .append("\n");
    } else {
      sb.append("\t\t\treturn new RayObjects").append(Rcount)
          .append("(Ray.internal().call(null, () -> f.apply(").append(nulls).append("), ")
          .append(Rcount).append(parameterUse).append(").objs);")
          .append("\n");
    }
    sb.append("\t\t}\n");
    sb.append("\t}\n");
    return sb.toString();
  }

  private static Set<Set<Integer>> whichTisFutureComposition(int Tcount) {
    Set<Set<Integer>> ret = new HashSet<>();
    Set<Integer> N = new HashSet<>();
    for (int k = 0; k < Tcount; k++) {
      N.add(k);
    }
    for (int k = 0; k <= Tcount; k++) {
      ret.addAll(CNn(N, k));
    }
    return ret;
  }

  //pick n numbers in N
  private static Set<Set<Integer>> CNn(Set<Integer> N, int n) {
    C c = new C();
    for (int k = 0; k < n; k++) {
      c.mul(N);
    }
    return c.v;
  }

  static class C {

    Set<Set<Integer>> v;

    public C() {
      v = new HashSet<>();
      v.add(new HashSet<>());
    }

    void mul(Set<Integer> ns) {
      Set<Set<Integer>> ret = new HashSet<>();
      for (int n : ns) {
        ret.addAll(mul(n));
      }
      this.v = ret;
    }

    Set<Set<Integer>> mul(int n) {
      Set<Set<Integer>> ret = new HashSet<>();
      for (Set<Integer> s : v) {
        if (s.contains(n)) {
          continue;
        }
        Set<Integer> ns = new HashSet<>(s);
        ns.add(n);
        ret.add(ns);
      }
      return ret;
    }
  }

}
