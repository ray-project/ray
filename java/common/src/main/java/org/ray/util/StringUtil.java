package org.ray.util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Vector;

public class StringUtil {

  /**
   * split.
   * @param s         input string
   * @param splitters common splitters
   * @param open      open braces
   * @param close     close braces
   * @return output array list
   */
  public static Vector<String> split(String s, String splitters, String open, String close) {
    // The splits.
    Vector<String> split = new Vector<>();
    // The stack.
    ArrayList<Start> stack = new ArrayList<>();

    int lastPos = 0;

    // Walk the string.
    for (int i = 0; i < s.length(); i++) {
      // Get the char there.
      char ch = s.charAt(i);

      // Is it an open brace?
      int o = open.indexOf(ch);

      // Is it a close brace?
      int c = close.indexOf(ch);

      // Is it a splitter?
      int sp = splitters.indexOf(ch);

      if (stack.size() == 0 && sp >= 0) {
        if (i == lastPos) {
          ++lastPos;
          continue;
        }

        split.add(s.substring(lastPos, i));
        lastPos = i + 1;
      } else if (o >= 0 && (c < 0 || stack.size() == 0)) {
        // Its an open! Push it.
        stack.add(new Start(o, i));
      } else if (c >= 0 && stack.size() > 0) {
        // Pop (if matches).
        int tosPos = stack.size() - 1;
        Start tos = stack.get(tosPos);
        // Does the brace match?
        if (tos.brace == c) {
          // Done with that one.
          stack.remove(tosPos);
        }
      }
    }

    if (lastPos < s.length()) {
      split.add(s.substring(lastPos, s.length()));
    }

    // build removal filter set
    HashSet<Character> removals = new HashSet<>();
    for (int i = 0; i < splitters.length(); i++) {
      removals.add(splitters.charAt(i));
    }

    for (int i = 0; i < open.length(); i++) {
      removals.add(open.charAt(i));
    }

    for (int i = 0; i < close.length(); i++) {
      removals.add(close.charAt(i));
    }

    // apply removal filter set
    for (int i = 0; i < split.size(); i++) {
      String cs = split.get(i);

      // remove heading chars
      int j;
      for (j = 0; j < cs.length(); j++) {
        if (!removals.contains(cs.charAt(j))) {
          break;
        }
      }
      cs = cs.substring(j);

      // remove tail chars
      for (j = cs.length() - 1; j >= 0; j--) {
        if (!removals.contains(cs.charAt(j))) {
          break;
        }
      }
      cs = cs.substring(0, j + 1);

      // reset cs
      split.set(i, cs);
    }

    return split;
  }

  public static boolean isNullOrEmpty(String s) {
    return s == null || s.length() == 0;
  }

  public static <T> String mergeArray(T[] objs, String concatenator) {
    StringBuilder sb = new StringBuilder();
    for (T obj : objs) {
      sb.append(obj).append(concatenator);
    }
    return objs.length == 0 ? "" : sb.substring(0, sb.length() - concatenator.length());
  }

  // Holds the start of an element and which brace started it.
  private static class Start {

    // The brace number from the braces string in use.
    final int brace;
    // The position in the string it was seen.
    final int pos;

    // Constructor.
    public Start(int brace, int pos) {
      this.brace = brace;
      this.pos = pos;
    }
  }
}

