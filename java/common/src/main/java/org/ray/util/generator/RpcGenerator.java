package org.ray.util.generator;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.ray.util.FileUtil;
import org.ray.util.generator.Composition.Tr;

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
    for (Tr tr : Composition.calculate(Share.MAX_T, Share.MAX_R)) {
      buildCall(sb, tr.tcount, tr.rcount);
    }
    sb.append("}\n");
    return sb.toString();
  }

  private static void buildCall(StringBuilder sb, int tcount, int rcount) {
    for (Set<Integer> whichTisFuture : whichTisFutureComposition(tcount)) {
      sb.append(buildCall(tcount, rcount, whichTisFuture));
    }
  }

  private static String buildCall(int tcount, int rcount, Set<Integer> whichTisFuture) {
    StringBuilder sb = new StringBuilder();
    String parameter = (tcount == 0 ? ""
        : ", " + Share.buildParameter(tcount, "T", whichTisFuture));
    sb.append("\tpublic static <").append(Share.buildClassDeclare(tcount, rcount)).append("> ")
        .append(Share.buildRpcReturn(rcount)).append(" call")
        .append(rcount == 1 ? "" : (rcount <= 0 ? "_n" : ("_" + rcount))).append("(RayFunc_")
        .append(tcount).append("_")
        .append(rcount <= 0 ? (rcount == 0 ? "n" : "n_list") : rcount).append("<")
        .append(Share.buildClassDeclare(tcount, rcount)).append("> f").append(
        rcount <= 0 ? (rcount == 0 ? ", Collection<RID> returnids" : ", Integer returnCount")
            : "").append(parameter).append(") {\n");

    String nulls = Share.buildRepeat("null",
        tcount + (rcount == 0 ? 1/*for first arg map*/ : 0));
    String parameterUse = (tcount == 0 ? "" : (", " + Share.buildParameterUse(tcount, "T")));
    String labmdaUse = "RayFunc_"
        + tcount + "_" + (rcount <= 0 ? (rcount == 0 ? "n" : "n_list") : rcount)
        + ".class, f";

    sb.append("\t\tif (Ray.Parameters().remoteLambda()) {\n");
    if (rcount == 1) {
      sb.append("\t\t\treturn Ray.internal().call(null, ").append(labmdaUse).append(", 1")
          .append(parameterUse).append(").objs[0];")
          .append("\n");
    } else if (rcount == 0) {
      sb.append("\t\t\treturn Ray.internal().callWithReturnLabels(null, ")
          .append(labmdaUse).append(", returnids").append(parameterUse).append(");")
          .append("\n");
    } else if (rcount < 0) {
      sb.append("\t\t\treturn Ray.internal().callWithReturnIndices(null, ")
          .append(labmdaUse).append(", returnCount").append(parameterUse).append(");")
          .append("\n");
    } else {
      sb.append("\t\t\treturn new RayObjects").append(rcount)
          .append("(Ray.internal().call(null, ").append(labmdaUse).append(", ").append(rcount)
          .append(parameterUse).append(").objs);")
          .append("\n");
    }
    sb.append("\t\t} else {\n");
    if (rcount == 1) {
      sb.append("\t\t\treturn Ray.internal().call(null, () -> f.apply(").append(nulls)
          .append("), 1").append(parameterUse).append(").objs[0];")
          .append("\n");
    } else if (rcount == 0) {
      sb.append("\t\t\treturn Ray.internal().callWithReturnLabels(null, () -> f.apply(")
          .append(nulls).append("), returnids").append(parameterUse).append(");")
          .append("\n");
    } else if (rcount < 0) {
      sb.append("\t\t\treturn Ray.internal().callWithReturnIndices(null, () -> f.apply(")
          .append(nulls).append("), returnCount").append(parameterUse).append(");")
          .append("\n");
    } else {
      sb.append("\t\t\treturn new RayObjects").append(rcount)
          .append("(Ray.internal().call(null, () -> f.apply(").append(nulls).append("), ")
          .append(rcount).append(parameterUse).append(").objs);")
          .append("\n");
    }
    sb.append("\t\t}\n");
    sb.append("\t}\n");
    return sb.toString();
  }

  private static Set<Set<Integer>> whichTisFutureComposition(int tcount) {
    Set<Set<Integer>> ret = new HashSet<>();
    Set<Integer> n = new HashSet<>();
    for (int k = 0; k < tcount; k++) {
      n.add(k);
    }
    for (int k = 0; k <= tcount; k++) {
      ret.addAll(cnn(n, k));
    }
    return ret;
  }

  //pick n numbers in N
  private static Set<Set<Integer>> cnn(Set<Integer> bigN, int n) {
    C c = new C();
    for (int k = 0; k < n; k++) {
      c.mul(bigN);
    }
    return c.vc;
  }

  static class C {

    Set<Set<Integer>> vc;

    public C() {
      vc = new HashSet<>();
      vc.add(new HashSet<>());
    }

    void mul(Set<Integer> ns) {
      Set<Set<Integer>> ret = new HashSet<>();
      for (int n : ns) {
        ret.addAll(mul(n));
      }
      this.vc = ret;
    }

    Set<Set<Integer>> mul(int n) {
      Set<Set<Integer>> ret = new HashSet<>();
      for (Set<Integer> s : vc) {
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
