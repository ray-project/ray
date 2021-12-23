package io.ray.serialization.codegen;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import io.ray.serialization.Fury;
import io.ray.serialization.bean.Foo;
import io.ray.serialization.encoder.SeqCodecBuilder;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.codehaus.janino.ByteArrayClassLoader;
import org.testng.Assert;
import org.testng.annotations.Test;

public class CodeGeneratorTest {

  @Test
  public void tryDuplicateCompileConcurrent() throws InterruptedException {
    ExecutorService executorService = Executors.newFixedThreadPool(10);
    ByteArrayClassLoader classLoader = new ByteArrayClassLoader(new HashMap<>());
    AtomicBoolean hasException = new AtomicBoolean(false);
    AtomicReference<ClassLoader> prevLoader = new AtomicReference<>();
    for (int i = 0; i < 1000; i++) {
      executorService.execute(
          () -> {
            try {
              ClassLoader newLoader = tryDuplicateCompile(classLoader);
              if (prevLoader.get() != null) {
                Assert.assertSame(newLoader, prevLoader.get());
                prevLoader.set(newLoader);
              }
            } catch (Exception e) {
              hasException.set(true);
            }
          });
    }
    executorService.shutdown();
    assertTrue(executorService.awaitTermination(30, TimeUnit.SECONDS));
    assertFalse(hasException.get());
  }

  @Test
  public void tryDuplicateCompile() {
    tryDuplicateCompile(new ByteArrayClassLoader(new HashMap<>()));
  }

  public ClassLoader tryDuplicateCompile(ClassLoader loader) {
    CodeGenerator codeGenerator = CodeGenerator.getSharedCodeGenerator(loader);
    SeqCodecBuilder codecBuilder = new SeqCodecBuilder(Foo.class, Fury.builder().build());
    CompileUnit compileUnit =
        new CompileUnit(
            Foo.class.getPackage().getName(),
            codecBuilder.codecClassName(Foo.class),
            codecBuilder::genCode);
    ClassLoader loader1 = codeGenerator.compile(compileUnit);
    ClassLoader loader2 = codeGenerator.compile(compileUnit);
    Assert.assertSame(loader1, loader2);
    return loader1;
  }

  @Test
  public void tryDefineClassesInClassLoader() {
    ByteArrayClassLoader loader = new ByteArrayClassLoader(new HashMap<>());
    SeqCodecBuilder codecBuilder = new SeqCodecBuilder(Foo.class, Fury.builder().build());
    CompileUnit compileUnit =
        new CompileUnit(
            Foo.class.getPackage().getName(),
            codecBuilder.codecClassName(Foo.class),
            codecBuilder::genCode);
    Map<String, byte[]> byteCodeMap = JaninoUtils.toBytecode(loader, compileUnit);
    byte[] byteCodes = byteCodeMap.get(CodeGenerator.classFilepath(compileUnit));
    Assert.assertTrue(
        CodeGenerator.tryDefineClassesInClassLoader(
            loader, CodeGenerator.fullClassName(compileUnit), byteCodes));
    Assert.assertFalse(
        CodeGenerator.tryDefineClassesInClassLoader(
            loader, CodeGenerator.fullClassName(compileUnit), byteCodes));
  }

  @Test
  public void classFilepath() {
    String p =
        CodeGenerator.classFilepath(
            new CompileUnit(Foo.class.getPackage().getName(), Foo.class.getSimpleName(), ""));
    Assert.assertEquals(
        p,
        String.format(
            "%s/%s.class",
            Foo.class.getPackage().getName().replace(".", "/"), Foo.class.getSimpleName()));
  }

  @Test
  public void fullClassName() {
    CompileUnit unit =
        new CompileUnit(Foo.class.getPackage().getName(), Foo.class.getSimpleName(), "");
    Assert.assertEquals(CodeGenerator.fullClassName(unit), Foo.class.getName());
  }
}
