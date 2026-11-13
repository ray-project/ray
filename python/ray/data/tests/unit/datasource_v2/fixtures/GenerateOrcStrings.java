// Test fixture generator. Requires org.apache.orc:orc-tools:2.2.1:uber.
// Run with JDK 17; see README.md in this directory.
import java.nio.charset.StandardCharsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.storage.ql.exec.vector.BytesColumnVector;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;

public class GenerateOrcStrings {
  public static void main(String[] args) throws Exception {
    TypeDescription schema = TypeDescription.fromString("struct<s:string>");
    OrcFile.WriterOptions options =
        OrcFile.writerOptions(new Configuration()).setSchema(schema).compress(CompressionKind.ZLIB);
    try (Writer writer = OrcFile.createWriter(new Path(args[0]), options)) {
      String[][] stripes = {
        {"a", "a\u0000b", "a\u0000z", "aé"},
        {"m", "mé", "中", "\uD83D\uDE00"},
        {"z".repeat(1100), "z".repeat(1100) + "é", null, "\uD83D\uDE01"}
      };
      for (String[] values : stripes) {
        VectorizedRowBatch batch = schema.createRowBatch();
        BytesColumnVector column = (BytesColumnVector) batch.cols[0];
        for (String value : values) {
          int row = batch.size++;
          if (value == null) {
            column.noNulls = false;
            column.isNull[row] = true;
          } else {
            column.setVal(row, value.getBytes(StandardCharsets.UTF_8));
          }
        }
        writer.addRowBatch(batch);
        // Deliberately force three tiny stripes, independent of compression.
        writer.writeIntermediateFooter();
      }
    }
  }
}
