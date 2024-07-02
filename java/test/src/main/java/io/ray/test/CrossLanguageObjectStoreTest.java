package io.ray.test;

import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.api.function.PyFunction;
import io.ray.runtime.util.ArrowUtil;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = {"cluster"})
public class CrossLanguageObjectStoreTest extends BaseTest {

  private static final String PYTHON_MODULE = "test_cross_language_invocation";
  private static final int vecSize = 5;

  @BeforeClass
  public void beforeClass() {
    // Delete and re-create the temp dir.
    File tempDir =
        new File(
            System.getProperty("java.io.tmpdir")
                + File.separator
                + "ray_cross_language_object_store_test");
    FileUtils.deleteQuietly(tempDir);
    tempDir.mkdirs();
    tempDir.deleteOnExit();

    // Write the test Python file to the temp dir.
    InputStream in =
        CrossLanguageObjectStoreTest.class.getResourceAsStream(
            File.separator + PYTHON_MODULE + ".py");
    File pythonFile = new File(tempDir.getAbsolutePath() + File.separator + PYTHON_MODULE + ".py");
    try {
      FileUtils.copyInputStreamToFile(in, pythonFile);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    System.setProperty(
        "ray.job.code-search-path",
        System.getProperty("java.class.path") + File.pathSeparator + tempDir.getAbsolutePath());
  }

  @Test
  public void testPythonPutAndJavaGet() {
    ObjectRef<VectorSchemaRoot> res =
        Ray.task(PyFunction.of(PYTHON_MODULE, "py_put_into_object_store", VectorSchemaRoot.class))
            .remote();
    VectorSchemaRoot root = res.get();
    BigIntVector newVector = (BigIntVector) root.getVector(0);
    for (int i = 0; i < vecSize; i++) {
      Assert.assertEquals(i, newVector.get(i));
    }
  }

  @Test
  public void testJavaPutAndPythonGet() {
    BigIntVector vector = new BigIntVector("ArrowBigIntVector", ArrowUtil.rootAllocator);
    vector.setValueCount(vecSize);
    for (int i = 0; i < vecSize; i++) {
      vector.setSafe(i, i);
    }
    List<Field> fields = Arrays.asList(vector.getField());
    List<FieldVector> vectors = Arrays.asList(vector);
    VectorSchemaRoot root = new VectorSchemaRoot(fields, vectors);
    ObjectRef<VectorSchemaRoot> obj = Ray.put(root);

    ObjectRef<VectorSchemaRoot> res =
        Ray.task(
                PyFunction.of(
                    PYTHON_MODULE, "py_object_store_get_and_check", VectorSchemaRoot.class),
                obj)
            .remote();

    VectorSchemaRoot newRoot = res.get();
    BigIntVector newVector = (BigIntVector) newRoot.getVector(0);
    for (int i = 0; i < vecSize; i++) {
      Assert.assertEquals(i, newVector.get(i));
    }
  }
}
