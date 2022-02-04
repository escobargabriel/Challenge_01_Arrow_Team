import java.io.FileNotFoundException;
import java.util.List;
import org.apache.arrow.vector.VectorSchemaRoot;

public class MainProgram {
  public static void main(String[] args) throws FileNotFoundException {
    String path = args[0];
    DataManipulation data = new DataManipulation();
    List list = data.readCSVFile(path);
    try(VectorSchemaRoot vectorSchemaRoot = data.vectorToAVectorSchemaRoot(list)) {
      data.vectorSchemaRootToACsvFileTransform(vectorSchemaRoot);
    }
  }
}