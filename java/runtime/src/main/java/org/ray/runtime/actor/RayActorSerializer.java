package org.ray.runtime.actor;

import java.io.IOException;
import org.nustaq.serialization.FSTBasicObjectSerializer;
import org.nustaq.serialization.FSTClazzInfo;
import org.nustaq.serialization.FSTClazzInfo.FSTFieldInfo;
import org.nustaq.serialization.FSTObjectInput;
import org.nustaq.serialization.FSTObjectOutput;

public class RayActorSerializer extends FSTBasicObjectSerializer {

  @Override
  public void writeObject(FSTObjectOutput out, Object toWrite, FSTClazzInfo clzInfo,
      FSTClazzInfo.FSTFieldInfo referencedBy, int streamPosition) throws IOException {
    ((NativeRayActor) toWrite).fork().writeExternal(out);
  }

  @Override
  public void readObject(FSTObjectInput in, Object toRead, FSTClazzInfo clzInfo,
                         FSTFieldInfo referencedBy) throws Exception {
    super.readObject(in, toRead, clzInfo, referencedBy);
    ((NativeRayActor) toRead).readExternal(in);
  }
}
