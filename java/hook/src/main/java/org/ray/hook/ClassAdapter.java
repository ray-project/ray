package org.ray.hook;

import java.util.Set;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassWriter;

public class ClassAdapter {

  public static Result hookClass(ClassLoader loader, String className, byte[] classfileBuffer) {
    // we have to comment out this quick filter as this is not accurate
    // e.g., org/ray/api/test/ActorTest$Adder.class is skipped!!!
    // even worse, this is non-deterministic...

    ClassReader reader = new ClassReader(classfileBuffer);
    ClassWriter writer = new ClassWriter(reader, 0);
    ClassDetectVisitor pre = new ClassDetectVisitor(loader, writer, className);
    byte[] result;

    reader.accept(pre, ClassReader.SKIP_FRAMES);
    if (pre.detectedMethods().isEmpty() && pre.actorCalls() == 0) {
      result = classfileBuffer;
    } else {
      if (pre.actorCalls() > 0) {
        reader = new ClassReader(writer.toByteArray());
      }

      writer = new ClassWriter(reader, ClassWriter.COMPUTE_FRAMES);
      reader.accept(new ClassOverrideVisitor(writer, className, pre.detectedMethods()),
          ClassReader.SKIP_FRAMES);
      result = writer.toByteArray();
    }

    Result rr = new Result();
    rr.changedMethods = pre.detectedMethods();
    rr.classBuffer = result;
    return rr;
  }

  public static class Result {

    public byte[] classBuffer;
    public Set<MethodId> changedMethods;
  }
}
