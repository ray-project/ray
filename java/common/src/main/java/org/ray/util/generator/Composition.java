package org.ray.util.generator;

import java.util.ArrayList;
import java.util.List;

/**
 * Calculate all compositions for Parameter's count + Return's count, this class is used by code
 * generators.
 */
public class Composition {

  public static class TR {

    public final int Tcount;
    public final int Rcount;

    public TR(int tcount, int rcount) {
      super();
      Tcount = tcount;
      Rcount = rcount;
    }
  }

  public static List<TR> calculate(int maxT, int maxR) {
    List<TR> ret = new ArrayList<>();
    for (int t = 0; t <= maxT; t++) {

      // <= 0 for dynamic return count
      // 0 for call_n returns RayMap<RID, x>
      // -1 for call_n returns RayObject<>[N]

      for (int r = -1; r <= maxR; r++) {
        ret.add(new TR(t, r));
      }
    }
    return ret;
  }
}
