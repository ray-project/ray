package org.ray.runtime.util;

import java.io.IOException;
import org.nustaq.serialization.FSTBasicObjectSerializer;
import org.nustaq.serialization.FSTClazzInfo;
import org.nustaq.serialization.FSTClazzInfo.FSTFieldInfo;
import org.nustaq.serialization.FSTObjectInput;
import org.nustaq.serialization.FSTObjectOutput;
import org.ray.api.RayActor;
import org.ray.runtime.RayActorImpl;

public class RayActorSerializer extends FSTBasicObjectSerializer {

  @Override
  public void writeObject(FSTObjectOutput out, Object toWrite, FSTClazzInfo clzInfo,
      FSTClazzInfo.FSTFieldInfo referencedBy, int streamPosition) throws IOException {
    ((RayActorImpl) toWrite).fork(false).writeExternal(out);
  }

  @Override
  public void readObject(FSTObjectInput in, Object toRead, FSTClazzInfo clzInfo,
                         FSTFieldInfo referencedBy) throws Exception {
    super.readObject(in, toRead, clzInfo, referencedBy);
    ((RayActorImpl) toRead).readExternal(in);
  }
}

