import static java.util.Arrays.asList;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;

public class DataManipulation {
  public static final RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);

  public List<FieldVector> readCSVFile(String path) throws FileNotFoundException {
    final VarCharVector nameVector = new VarCharVector("name", allocator);
    final VarCharVector teamVector = new VarCharVector("team", allocator);
    final VarCharVector positionVector = new VarCharVector("position", allocator);
    final VarCharVector heightVector = new VarCharVector("height", allocator);
    final VarCharVector weightVector = new VarCharVector("weight", allocator);
    final VarCharVector ageVector = new VarCharVector("age", allocator);
    String line;
    try (BufferedReader br = new BufferedReader(new FileReader(path))) {
      int i = 0;
      while ((line = br.readLine()) != null) {
        String[] values = line.split(",");
        nameVector.setSafe(i, values[0].getBytes());
        teamVector.setSafe(i, values[1].getBytes());
        positionVector.setSafe(i, values[2].getBytes());
        heightVector.setSafe(i, values[3].getBytes());
        weightVector.setSafe(i, values[4].getBytes());
        ageVector.setSafe(i, values[5].getBytes());
        ++i;
      }
    } catch (IOException ex) {
      ex.printStackTrace();
    }
    List<FieldVector> list = asList(nameVector, teamVector,
      positionVector, heightVector, weightVector, ageVector);
    return list;
  }

  public VectorSchemaRoot vectorToAVectorSchemaRoot(List list){
    try(final VectorSchemaRoot vectorSchemaRoot = new VectorSchemaRoot(list)) {
      vectorSchemaRoot.setRowCount(1035);
      return vectorSchemaRoot;
    }
  }

  public void vectorSchemaRootToACsvFileTransform(VectorSchemaRoot vectorSchemaRoot) {
    VarCharVector vectorName = (VarCharVector) vectorSchemaRoot.getVector(0);
    VarCharVector vectorTeam = (VarCharVector) vectorSchemaRoot.getVector(1);
    VarCharVector vectorPosition = (VarCharVector) vectorSchemaRoot.getVector(2);
    VarCharVector vectorHeight = (VarCharVector) vectorSchemaRoot.getVector(3);
    VarCharVector vectorWeight = (VarCharVector) vectorSchemaRoot.getVector(4);
    VarCharVector vectorAge = (VarCharVector) vectorSchemaRoot.getVector(5);
    int aux = 0;
    try {
      FileWriter csvWriter = new FileWriter("result.csv");
      while (aux < 1036) {
       csvWriter.append((CharSequence) vectorName.getObject(aux));
       csvWriter.append(",");
       csvWriter.append((CharSequence) vectorTeam.getObject(aux));
       csvWriter.append(",");
       csvWriter.append((CharSequence) vectorPosition.getObject(aux));
       csvWriter.append(",");
       csvWriter.append((CharSequence) vectorHeight.getObject(aux));
       csvWriter.append(",");
       csvWriter.append((CharSequence) vectorWeight.getObject(aux));
       csvWriter.append(",");
       csvWriter.append((CharSequence) vectorAge.getObject(aux));
        ++aux;
      }
      csvWriter.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}

