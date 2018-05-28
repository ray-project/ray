package org.ray.util;

import java.util.Random;

/**
 * Common utilities
 */
public class CommonUtil {

  private static final Random seed = new Random();

  /**
   * Get random number between 0 and (max-1)
   */
  public static int getRandom(int max) {
    return Math.abs(seed.nextInt() % max);
  }

}
