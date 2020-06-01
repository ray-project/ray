package io.ray.runtime.actor;

import java.io.IOException;
import org.nustaq.serialization.FSTBasicObjectSerializer;
import org.nustaq.serialization.FSTClazzInfo;
import org.nustaq.serialization.FSTClazzInfo.FSTFieldInfo;
import org.nustaq.serialization.FSTObjectInput;
import org.nustaq.serialization.FSTObjectOutput;

/**
 * To deal with serialization about {@link NativeRayActor}.
 */
public class NativeRayActorSerializer extends FSTBasicObjectSerializer {

  @Override
  public void writeObject(FSTObjectOutput out, Object toWrite, FSTClazzInfo clzInfo,
      FSTClazzInfo.FSTFieldInfo referencedBy, int streamPosition) throws IOException {
    ((NativeRayActor) toWrite).writeExternal(out);
  }

  @Override
  public void readObject(FSTObjectInput in, Object toRead, FSTClazzInfo clzInfo,
      FSTFieldInfo referencedBy) throws Exception {
    super.readObject(in, toRead, clzInfo, referencedBy);
    ((NativeRayActor) toRead).readExternal(in);
  }
}
