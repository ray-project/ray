package org.ray.api.test;

import java.io.IOException;
import java.util.zip.DataFormatException;
import org.ray.hook.JarRewriter;

public class RewriteTest {

  public static void main(String[] args) throws IOException, DataFormatException {
    System.out.println(System.getProperty("user.dir"));
    JarRewriter.rewrite("target", "target2");
  }
}
