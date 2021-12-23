package io.ray.serialization.codegen;

import com.google.common.reflect.TypeToken;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

public class CodegenContextTest {

  public static class A {
    public String f1;
  }

  @Test
  public void type() {
    CodegenContext ctx = new CodegenContext();
    TypeToken<List<List<String>>> typeToken = new TypeToken<List<List<String>>>() {};
    String type = ctx.type(typeToken);
    Assert.assertEquals("List", type);
    Assert.assertEquals("int[][]", ctx.type(int[][].class));
  }

  @Test
  public void testTypeForInnerClass() {
    CodegenContext ctx = new CodegenContext();
    Assert.assertEquals(ctx.type(A.class), A.class.getCanonicalName());
    ctx.addImport(getClass());
    Assert.assertEquals(ctx.type(A.class), A.class.getCanonicalName());
    ctx.addImport(A.class);
    Assert.assertEquals(ctx.type(A.class), A.class.getSimpleName());
  }
}
