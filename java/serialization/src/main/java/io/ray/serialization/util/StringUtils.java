package io.ray.serialization.util;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class StringUtils {

  private static final char[] DIGITS_LOWER = {
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'
  };

  public static char[] encodeHex(final byte[] data) {
    final int l = data.length;
    final char[] out = new char[l << 1];
    // two characters form the hex value.
    for (int i = 0, j = 0; i < l; i++) {
      out[j++] = DIGITS_LOWER[(0xF0 & data[i]) >>> 4];
      out[j++] = DIGITS_LOWER[0x0F & data[i]];
    }
    return out;
  }

  public static String encodeHexString(final byte[] data) {
    return new String(encodeHex(data));
  }

  public static String capitalize(final String str) {
    int strLen;
    if (str == null || (strLen = str.length()) == 0) {
      return str;
    }

    final int firstCodepoint = str.codePointAt(0);
    final int newCodePoint = Character.toTitleCase(firstCodepoint);
    if (firstCodepoint == newCodePoint) {
      // already capitalized
      return str;
    }

    final int[] newCodePoints = new int[strLen]; // cannot be longer than the char array
    int outOffset = 0;
    newCodePoints[outOffset++] = newCodePoint; // copy the first codepoint
    for (int inOffset = Character.charCount(firstCodepoint); inOffset < strLen; ) {
      final int codepoint = str.codePointAt(inOffset);
      newCodePoints[outOffset++] = codepoint; // copy the remaining ones
      inOffset += Character.charCount(codepoint);
    }
    return new String(newCodePoints, 0, outOffset);
  }

  /** Uncapitalizes a String, changing the first character to lower case */
  public static String uncapitalize(final String str) {
    int strLen;
    if (str == null || (strLen = str.length()) == 0) {
      return str;
    }

    final int firstCodepoint = str.codePointAt(0);
    final int newCodePoint = Character.toLowerCase(firstCodepoint);
    if (firstCodepoint == newCodePoint) {
      // already capitalized
      return str;
    }

    final int[] newCodePoints = new int[strLen]; // cannot be longer than the char array
    int outOffset = 0;
    newCodePoints[outOffset++] = newCodePoint; // copy the first codepoint
    for (int inOffset = Character.charCount(firstCodepoint); inOffset < strLen; ) {
      final int codepoint = str.codePointAt(inOffset);
      newCodePoints[outOffset++] = codepoint; // copy the remaining ones
      inOffset += Character.charCount(codepoint);
    }
    return new String(newCodePoints, 0, outOffset);
  }

  public static String format(String str, Object... args) {
    StringBuilder builder = new StringBuilder(str);
    if (args.length % 2 != 0) {
      throw new IllegalArgumentException(
          "args length must be multiple of 2, but get " + args.length);
    }
    Map<String, String> values = new HashMap<>();
    for (int i = 0; i < args.length; i += 2) {
      values.put(args[i].toString(), args[i + 1].toString());
    }

    for (Map.Entry<String, String> entry : values.entrySet()) {
      int start;
      String pattern = "${" + entry.getKey() + "}";
      String value = entry.getValue();

      // Replace every occurrence of %(key) with value
      while ((start = builder.indexOf(pattern)) != -1) {
        builder.replace(start, start + pattern.length(), value);
      }
    }

    return builder.toString();
  }

  public static String stripBlankLines(String str) {
    StringBuilder builder = new StringBuilder();
    String[] split = str.split("\n");
    for (String s : split) {
      if (!s.trim().isEmpty()) {
        builder.append(s).append('\n');
      }
    }
    if (builder.charAt(builder.length() - 1) == '\n') {
      return builder.toString();
    } else {
      return builder.substring(0, builder.length() - 1);
    }
  }

  public static String random(int size) {
    char[] chars = new char[size];
    char start = ' ', end = 'z' + 1;
    int gap = end - start;
    ThreadLocalRandom random = ThreadLocalRandom.current();
    for (int i = 0; i < size; i++) {
      chars[i] = (char) (start + random.nextInt(gap));
    }
    return new String(chars);
  }

  public static boolean isBlank(final CharSequence cs) {
    int strLen;
    if (cs == null || (strLen = cs.length()) == 0) {
      return true;
    }
    for (int i = 0; i < strLen; i++) {
      if (!Character.isWhitespace(cs.charAt(i))) {
        return false;
      }
    }
    return true;
  }

  public static boolean isNotBlank(final CharSequence cs) {
    return !isBlank(cs);
  }
}
