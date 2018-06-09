package org.ray.util.generator;

import java.util.ArrayList;
import java.util.List;

/**
 * Calculate all compositions for Parameter's count + Return's count, this class is used by code
 * generators.
 */
public class Composition {

  public static List<Tr> calculate(int maxT, int maxR) {
    List<Tr> ret = new ArrayList<>();
    for (int t = 0; t <= maxT; t++) {

      // <= 0 for dynamic return count
      // 0 for call_n returns RayMap<RID, x>
      // -1 for call_n returns RayObject<>[N]

      for (int r = -1; r <= maxR; r++) {
        ret.add(new Tr(t, r));
      }
    }
    return ret;
  }

  public static class Tr {

    public final int tcount;
    public final int rcount;

    public Tr(int tcount, int rcount) {
      super();
      this.tcount = tcount;
      this.rcount = rcount;
    }
  }
}
