import java.io.FileNotFoundException;
import java.util.List;
import org.apache.arrow.vector.VectorSchemaRoot;

public class MainProgram {
  public static void main(String[] args) throws FileNotFoundException {
    DataManipulation data = new DataManipulation();
    List list = data.readCSVFile(args[0]);
    //data.readCSVFile(args[0]);
    VectorSchemaRoot vectorSchemaRoot = data.vectorToAVectorSchemaRoot(list);
    data.vectorSchemaRootToACsvFileTransform(vectorSchemaRoot);
  }
}
