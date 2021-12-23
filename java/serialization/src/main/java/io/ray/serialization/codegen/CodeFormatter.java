package io.ray.serialization.codegen;

public class CodeFormatter {

  /** Format code to add line number for debug */
  public static String format(String code) {
    StringBuilder codeBuilder = new StringBuilder(code.length());
    String[] split = code.split("\n", -1);
    int lineCount = 0;
    for (int i = 0; i < split.length; i++) {
      lineCount++;
      codeBuilder.append(String.format("/* %04d */ ", lineCount));
      codeBuilder.append(split[i]).append('\n');
    }
    if (code.charAt(code.length() - 1) == '\n') {
      return codeBuilder.toString();
    } else {
      // remove extra newline character
      return codeBuilder.substring(0, codeBuilder.length() - 1);
    }
  }
}
