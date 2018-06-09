package org.ray.hook;

import java.util.Set;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

/**
 * rewrite phase 2.
 */
public class ClassOverrideVisitor extends ClassVisitor {

  final String className;
  final Set<MethodId> rayRemoteMethods;
  MethodVisitor clinitVisitor;

  public ClassOverrideVisitor(ClassVisitor origin, String className,
                              Set<MethodId> rayRemoteMethods) {
    super(Opcodes.ASM6, origin);
    this.className = className;
    this.rayRemoteMethods = rayRemoteMethods;
    this.clinitVisitor = null;
  }

  @Override
  public MethodVisitor visitMethod(int access, String name, String desc, String signature,
                                   String[] exceptions) {
    if ("<clinit>".equals(name) && clinitVisitor == null) {
      MethodVisitor mv = super.visitMethod(access, name, desc, signature, exceptions);
      clinitVisitor = new StaticBlockVisitor(mv);
      return clinitVisitor;// dispatch the ASM modifications (assign values to the preComputedxxx
      // static field) to the clinitVisitor
    }

    ClassVisitor current = this;
    MethodId m = new MethodId(className, name, desc, (access & Opcodes.ACC_STATIC) != 0, null);
    if (rayRemoteMethods.contains(m)) {
      if (m.isStaticMethod()) {
        return new MethodVisitor(api,
            super.visitMethod(access, name, desc, signature, exceptions)) {
          @Override
          public void visitCode() {
            // step 1: add a field for the function id of this method
            System.out.println("add field: " + m.getStaticHashValueFieldName());
            String fieldName = m.getStaticHashValueFieldName();
            current.visitField(Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC, fieldName, "[B",
                null, null);

            // step 2: rewrite current method so if MethodSwitcher returns true, returns the
            // added function id directly
            // else call the original method
            mv.visitFieldInsn(Opcodes.GETSTATIC, className, fieldName, "[B");
            mv.visitMethodInsn(Opcodes.INVOKESTATIC, "org/ray/hook/runtime/MethodSwitcher",
                "execute",
                "([B)Z", false);
            Label dorealwork = new Label();
            mv.visitJumpInsn(Opcodes.IFEQ, dorealwork);
            properReturn(sayReturnType(desc), mv);// proper return on
            // different types
            mv.visitLabel(dorealwork);
            mv.visitCode();// real work
          }
        };
      } else { // non-static
        return super.visitMethod(access, name, desc, signature, exceptions);
      }
    } else {
      return super.visitMethod(access, name, desc, signature, exceptions);
    }
  }

  @Override
  public void visitEnd() {
    if (clinitVisitor == null) { // works fine
      // Create an empty static block and let our method
      // visitor modify it the same way it modifies an
      // existing static block
      {
        MethodVisitor mv = super.visitMethod(Opcodes.ACC_STATIC, "<clinit>", "()V", null, null);
        mv = new StaticBlockVisitor(mv);
        mv.visitCode();
        mv.visitInsn(Opcodes.RETURN);
        mv.visitMaxs(0, 0);
        mv.visitEnd();
      }
    }

    // for each non-static method, create a method for returning hash
    for (MethodId mid : this.rayRemoteMethods) {
      if (!mid.isStaticMethod()) {

        // step 1: create a new method called method_name_function_id()
        System.out.println("add method: " + mid.getIdMethodName());
        MethodVisitor mv = super.visitMethod(Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC,
            mid.getIdMethodName(), mid.getIdMethodDesc(), null, null);
        mv.visitCode();
        Label l0 = new Label();
        mv.visitLabel(l0);

        // step 2: add a new static field as the function id of this method
        System.out.println("add field: " + mid.getStaticHashValueFieldName());
        String fieldName = mid.getStaticHashValueFieldName();
        this.visitField(Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC, fieldName, "[B",
            null, null);
        mv.visitFieldInsn(Opcodes.GETSTATIC, className, fieldName, "[B");

        // step 3: call method switcher, and returns the function id when the mode is rewritten
        mv.visitMethodInsn(Opcodes.INVOKESTATIC, "org/ray/hook/runtime/MethodSwitcher", "execute",
            "([B)Z", false);
        mv.visitInsn(Opcodes.POP);
        Label l1 = new Label();
        mv.visitLabel(l1);
        properReturn(sayReturnType(mid.getMethodDesc()), mv);

        Label l2 = new Label();
        mv.visitLabel(l2);

        org.objectweb.asm.Type[] args = org.objectweb.asm.Type
            .getArgumentTypes(mid.getIdMethodDesc());
        int argCount = args.length;
        mv.visitMaxs(2, argCount);
        mv.visitEnd();
      }
    }
  }

  private void properReturn(String returnType, MethodVisitor mv) {
    int returnCode;
    Object returnValue = null;
    switch (returnType) {
      case "V":
        mv.visitInsn(Opcodes.RETURN);
        return;
      case "I":
      case "B":
      case "S":
      case "Z": // int byte short boolean
        returnCode = Opcodes.IRETURN;
        returnValue = 0;
        break;
      case "J": // long
        returnCode = Opcodes.LRETURN;
        returnValue = 0L;
        break;
      case "D": // double
        returnCode = Opcodes.DRETURN;
        returnValue = 0D;
        break;
      case "F": // float
        returnCode = Opcodes.FRETURN;
        returnValue = 0F;
        break;
      default: // reference
        returnCode = Opcodes.ARETURN;
        break;
    }
    if (returnValue != null) {
      mv.visitLdcInsn(returnValue);
    } else {
      mv.visitInsn(Opcodes.ACONST_NULL);
    }
    mv.visitInsn(returnCode);
  }

  private String sayReturnType(String str) {
    int left = str.lastIndexOf(")") + 1;
    return str.substring(left);
  }

  // init the static added field in <clinit>
  // static {
  //     assign value to _hashOf_XXX
  // }
  class StaticBlockVisitor extends MethodVisitor {

    StaticBlockVisitor(MethodVisitor mv) {
      super(Opcodes.ASM6, mv);
    }

    @Override
    public void visitCode() {
      super.visitCode();

      // assign value for added hash fields within <clinit>
      for (MethodId m : rayRemoteMethods) {
        byte[] hash = m.getSha1Hash();
        insertByteArray(hash);
        mv.visitFieldInsn(Opcodes.PUTSTATIC, className, m.getStaticHashValueFieldName(), "[B");

        System.out.println("assign field: " + m.getStaticHashValueFieldName() + " = " + MethodId
            .toHexHashString(hash));
      }
    }

    private void insertByteArray(byte[] bytes) {
      int length = bytes.length;
      assert (length < Short.MAX_VALUE);
      mv.visitIntInsn(Opcodes.SIPUSH, length);
      mv.visitIntInsn(Opcodes.NEWARRAY, Opcodes.T_BYTE);
      mv.visitInsn(Opcodes.DUP);
      for (int i = 0; i < length; ++i) {
        mv.visitIntInsn(Opcodes.BIPUSH, i);
        mv.visitIntInsn(Opcodes.BIPUSH, bytes[i]);
        mv.visitInsn(Opcodes.BASTORE);
        if (i < (length - 1)) {
          mv.visitInsn(Opcodes.DUP);
        }
      }
    }

  }

}
