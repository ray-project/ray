package io.ray.runtime.util.generator;

public abstract class BaseGenerator {

  protected static final int MAX_PARAMETERS = 6;

  protected StringBuilder sb;

  protected void newLine(String line) {
    sb.append(line).append("\n");
  }

  protected void newLine(int numIndents, String line) {
    indents(numIndents);
    newLine(line);
  }

  protected void indents(int numIndents) {
    for (int i = 0; i < numIndents; i++) {
      sb.append("  ");
    }
  }
}
