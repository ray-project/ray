package org.ray.util.generator;

import java.util.Set;

/**
 * Share util for generators.
 */
public class Share {

  public static final int MAX_T = 6;
  public static final int MAX_R = 4;

  /**
   * T0, T1, T2, T3, R.
   */
  public static String buildClassDeclare(int tcount, int rcount) {
    StringBuilder sb = new StringBuilder();
    for (int k = 0; k < tcount; k++) {
      sb.append("T").append(k).append(", ");
    }
    if (rcount == 0) {
      sb.append("R, RID");
    } else if (rcount < 0) {
      assert (rcount == -1);
      sb.append("R");
    } else {
      for (int k = 0; k < rcount; k++) {
        sb.append("R").append(k).append(", ");
      }
      sb.deleteCharAt(sb.length() - 1);
      sb.deleteCharAt(sb.length() - 1);
    }
    return sb.toString();
  }

  /**
   * T0 t0, T1 t1, T2 t2, T3 t3.
   */
  public static String buildParameter(int tcount, String tr, Set<Integer> whichTisFuture) {
    StringBuilder sb = new StringBuilder();
    for (int k = 0; k < tcount; k++) {
      sb.append(whichTisFuture != null && whichTisFuture.contains(k)
          ? "RayObject<" + (tr + k) + ">" : (tr + k)).append(" ").append(tr.toLowerCase())
          .append(k).append(", ");
    }
    if (tcount > 0) {
      sb.deleteCharAt(sb.length() - 1);
      sb.deleteCharAt(sb.length() - 1);
    }
    return sb.toString();
  }

  /**
   * t0, t1, t2.
   */
  public static String buildParameterUse(int tcount, String tr) {
    StringBuilder sb = new StringBuilder();
    for (int k = 0; k < tcount; k++) {
      sb.append(tr.toLowerCase()).append(k).append(", ");
    }
    if (tcount > 0) {
      sb.deleteCharAt(sb.length() - 1);
      sb.deleteCharAt(sb.length() - 1);
    }
    return sb.toString();
  }

  public static String buildParameterUse2(int tcount, int startIndex, String tr, String pre,
                                          String post) {
    StringBuilder sb = new StringBuilder();
    for (int k = 0; k < tcount; k++) {
      sb.append("(").append(tr).append(k).append(")").append(pre).append(k + startIndex)
          .append(post).append(", ");
    }
    if (tcount > 0) {
      sb.deleteCharAt(sb.length() - 1);
      sb.deleteCharAt(sb.length() - 1);
    }
    return sb.toString();
  }

  public static String buildFuncReturn(int rcount) {
    if (rcount == 0) {
      return "Map<RID, R>";
    } else if (rcount < 0) {
      assert (-1 == rcount);
      return "List<R>";
    }
    if (rcount == 1) {
      return "R0";
    }
    StringBuilder sb = new StringBuilder();
    for (int k = 0; k < rcount; k++) {
      sb.append("R").append(k).append(", ");
    }
    sb.deleteCharAt(sb.length() - 1);
    sb.deleteCharAt(sb.length() - 1);
    return "MultipleReturns" + rcount + "<" + sb.toString() + ">";

  }

  public static String buildRpcReturn(int rcount) {
    if (rcount == 0) {
      return "RayMap<RID, R>";
    } else if (rcount < 0) {
      assert (rcount == -1);
      return "RayList<R>";
    }

    if (rcount == 1) {
      return "RayObject<R0>";
    }
    StringBuilder sb = new StringBuilder();
    for (int k = 0; k < rcount; k++) {
      sb.append("R").append(k).append(", ");
    }
    sb.deleteCharAt(sb.length() - 1);
    sb.deleteCharAt(sb.length() - 1);
    return "RayObjects" + rcount + "<" + sb.toString() + ">";

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
