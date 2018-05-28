package org.ray.util.generator;

import java.util.Set;

/**
 * Share util for generators
 */
public class Share {

  public static final int MAX_T = 6;
  public static final int MAX_R = 4;

  /**
   * T0, T1, T2, T3, R
   */
  public static String buildClassDeclare(int Tcount, int Rcount) {
    StringBuilder sb = new StringBuilder();
    for (int k = 0; k < Tcount; k++) {
      sb.append("T").append(k).append(", ");
    }
    if (Rcount == 0) {
      sb.append("R, RID");
    } else if (Rcount < 0) {
      assert (Rcount == -1);
      sb.append("R");
    } else {
      for (int k = 0; k < Rcount; k++) {
        sb.append("R").append(k).append(", ");
      }
      sb.deleteCharAt(sb.length() - 1);
      sb.deleteCharAt(sb.length() - 1);
    }
    return sb.toString();
  }

  /**
   * T0 t0, T1 t1, T2 t2, T3 t3
   */
  public static String buildParameter(int Tcount, String TR, Set<Integer> whichTisFuture) {
    StringBuilder sb = new StringBuilder();
    for (int k = 0; k < Tcount; k++) {
      sb.append(whichTisFuture != null && whichTisFuture.contains(k)
          ? "RayObject<" + (TR + k) + ">" : (TR + k)).append(" ").append(TR.toLowerCase())
          .append(k).append(", ");
    }
    if (Tcount > 0) {
      sb.deleteCharAt(sb.length() - 1);
      sb.deleteCharAt(sb.length() - 1);
    }
    return sb.toString();
  }

  /**
   * t0, t1, t2
   */
  public static String buildParameterUse(int Tcount, String TR) {
    StringBuilder sb = new StringBuilder();
    for (int k = 0; k < Tcount; k++) {
      sb.append(TR.toLowerCase()).append(k).append(", ");
    }
    if (Tcount > 0) {
      sb.deleteCharAt(sb.length() - 1);
      sb.deleteCharAt(sb.length() - 1);
    }
    return sb.toString();
  }

  public static String buildParameterUse2(int Tcount, int startIndex, String TR, String pre,
      String post) {
    StringBuilder sb = new StringBuilder();
    for (int k = 0; k < Tcount; k++) {
      sb.append("(").append(TR).append(k).append(")").append(pre).append(k + startIndex)
          .append(post).append(", ");
    }
    if (Tcount > 0) {
      sb.deleteCharAt(sb.length() - 1);
      sb.deleteCharAt(sb.length() - 1);
    }
    return sb.toString();
  }

  /**
   * MultipleReturns2<R0, R1> apply(); R0
   */
  public static String buildFuncReturn(int Rcount) {
    if (Rcount == 0) {
      return "Map<RID, R>";
    } else if (Rcount < 0) {
      assert (-1 == Rcount);
      return "List<R>";
    }
    if (Rcount == 1) {
      return "R0";
    }
    StringBuilder sb = new StringBuilder();
    for (int k = 0; k < Rcount; k++) {
      sb.append("R").append(k).append(", ");
    }
    sb.deleteCharAt(sb.length() - 1);
    sb.deleteCharAt(sb.length() - 1);
    return "MultipleReturns" + Rcount + "<" + sb.toString() + ">";

  }

  /**
   */
  public static String buildRpcReturn(int Rcount) {
    if (Rcount == 0) {
      return "RayMap<RID, R>";
    } else if (Rcount < 0) {
      assert (Rcount == -1);
      return "RayList<R>";
    }

    if (Rcount == 1) {
      return "RayObject<R0>";
    }
    StringBuilder sb = new StringBuilder();
    for (int k = 0; k < Rcount; k++) {
      sb.append("R").append(k).append(", ");
    }
    sb.deleteCharAt(sb.length() - 1);
    sb.deleteCharAt(sb.length() - 1);
    return "RayObjects" + Rcount + "<" + sb.toString() + ">";

  }

  public static String buildRepeat(String toRepeat, int count) {
    StringBuilder ret = new StringBuilder();
    for (int k = 0; k < count; k++) {
      ret.append(toRepeat).append(", ");
    }
    if (count > 0) {
      ret.deleteCharAt(ret.length() - 1);
      ret.deleteCharAt(ret.length() - 1);
    }
    return ret.toString();
  }
}
