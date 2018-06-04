package org.ray.hook;

import java.security.InvalidParameterException;
import java.util.HashSet;
import java.util.Set;
import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.Handle;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

/**
 * rewrite phase 1
 */
public class ClassDetectVisitor extends ClassVisitor {

  final String className;
  final Set<MethodId> rayMethods = new HashSet<>();
  boolean isActor = false;
  static int count = 0;
  int actorCalls = 0;
  final ClassLoader loader;

  public int actorCalls() {
    return actorCalls;
  }

  public ClassDetectVisitor(ClassLoader loader, ClassVisitor origin, String className) {
    super(Opcodes.ASM6, origin);
    this.className = className;
    this.loader = loader;
  }

  public Set<MethodId> detectedMethods() {
    return rayMethods;
  }

  @Override
  public void visitInnerClass(String name, String outerName,
      String innerName, int access) {
    // System.err.println("visist inner class " + outerName + "$" + innerName);
    super.visitInnerClass(name, outerName, innerName, access);
  }

  @Override
  public AnnotationVisitor visitAnnotation(String desc, boolean visible) {
    if (desc.contains("Lorg/ray/api/RayRemote;")) {
      isActor = true;
    }
    return super.visitAnnotation(desc, visible);
  }

  private void visitRayMethod(int access, String name, String mdesc) {
    if (name.equals("<init>")) {
      return;
    }

    MethodId m = new MethodId(className, name, mdesc, (access & Opcodes.ACC_STATIC) != 0, loader);
    rayMethods.add(m);
    //System.err.println("Visit " + m.toString());
    count++;
  }

  @Override
  public MethodVisitor visitMethod(int access, String name, String mdesc, String signature,
      String[] exceptions) {
    //System.out.println("Visit " + className + "." + name);
    if (isActor && (access & Opcodes.ACC_PUBLIC) != 0) {
      visitRayMethod(access, name, mdesc);
    }

    MethodVisitor origin = super.visitMethod(access, name, mdesc, signature, exceptions);
    return new MethodVisitor(this.api, origin) {
      @Override
      public AnnotationVisitor visitAnnotation(String adesc, boolean visible) {
        //handle rayRemote annotation
        if (adesc.contains("Lorg/ray/api/RayRemote;")) {
          visitRayMethod(access, name, mdesc);
        }
        return super.visitAnnotation(adesc, visible);
      }

      private boolean isValidCallParameterOrReturnType(Type t) {
        if (t.equals(Type.VOID_TYPE)) {
          return false;
        }
        if (t.equals(Type.BOOLEAN_TYPE)) {
          return false;
        }
        if (t.equals(Type.CHAR_TYPE)) {
          return false;
        }
        if (t.equals(Type.BYTE_TYPE)) {
          return false;
        }
        if (t.equals(Type.SHORT_TYPE)) {
          return false;
        }
        if (t.equals(Type.INT_TYPE)) {
          return false;
        }
        if (t.equals(Type.FLOAT_TYPE)) {
          return false;
        }
        if (t.equals(Type.LONG_TYPE)) {
          return false;
        }
        if (t.equals(Type.DOUBLE_TYPE)) {
          return false;
        }

        return true;
      }

      @Override
      public void visitInvokeDynamicInsn(String name, String desc, Handle bsm,
          Object... bsmArgs) {

        // fix all actor calls from InvokeVirtual to InvokeStatic
        if (desc.contains("org/ray/api/funcs/RayFunc_")) {
          int count = bsmArgs.length;
          for (int i = 0; i < count; ++i) {
            Object arg = bsmArgs[i];
            // System.err.println(arg.getClass().getName() + " " + arg.toString());
            if (arg.getClass().equals(Handle.class)) {
              Handle h = (Handle) arg;
              if (h.getTag() == Opcodes.H_INVOKEVIRTUAL) {
                String dsptr = h.getDesc();

                Type[] argTypes = Type.getArgumentTypes(dsptr);
                for (Type argt : argTypes) {
                  if (!isValidCallParameterOrReturnType(argt)) {
                    throw new InvalidParameterException(
                        "cannot use primitive parameter type '" + argt.getClassName()
                            + "' in method " + h.getOwner() + "." + h.getName());
                  }
                }
                Type retType = Type.getReturnType(dsptr);
                if (!isValidCallParameterOrReturnType(retType)) {
                  throw new InvalidParameterException(
                      "cannot use primitive return type '" + retType.getClassName() + "' in method "
                          + h.getOwner() + "." + h.getName());
                }

                dsptr = "(L" + h.getOwner() + ";" + dsptr.substring(1);
                Handle newh = new Handle(
                    Opcodes.H_INVOKESTATIC,
                    h.getOwner(),
                    h.getName() + MethodId.getFunctionIdPostfix,
                    dsptr,
                    h.isInterface());
                bsmArgs[i] = newh;
                //System.err.println("Change ray.call from " + h + " -> " + newh + ", isInterface = " + h.isInterface());
                ++actorCalls;
              }
            }
          }
        }
        super.visitInvokeDynamicInsn(name, desc, bsm, bsmArgs);
      }
    };
  }
}
