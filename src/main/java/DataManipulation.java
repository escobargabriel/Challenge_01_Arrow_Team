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
  VarCharVector nameVector = null;
  VarCharVector teamVector = null;
  VarCharVector positionVector = null;
  VarCharVector heightVector = null;
  VarCharVector weightVector = null;
  VarCharVector  ageVector = null;
  int rowCount = 0;

  public List readCSVFile(String path) throws FileNotFoundException {
    String line;
    nameVector     = new VarCharVector("name",     allocator);
    teamVector     = new VarCharVector("team",     allocator);
    positionVector = new VarCharVector("position", allocator);
    heightVector   = new VarCharVector("height",   allocator);
    weightVector   = new VarCharVector("weight",   allocator);
    ageVector      = new VarCharVector("age",      allocator);
    List<FieldVector> list ;
    try (BufferedReader br = new BufferedReader(new FileReader(path))) {
      int i = 0;
      while ((line = br.readLine()) != null) {
        String [] values = line.split(",");
        nameVector.setSafe(i, values[0].getBytes());
        teamVector.setSafe(i, values[1].getBytes());
        positionVector.setSafe(i, values[2].getBytes());
        heightVector.setSafe(i, values[3].getBytes());
        weightVector.setSafe(i, values[4].getBytes());
        ageVector.setSafe(i, values[5].getBytes());
        ++i;
        rowCount = i;
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return list = asList(nameVector, teamVector,
            positionVector, heightVector, weightVector, ageVector);
  }

  public VectorSchemaRoot vectorToAVectorSchemaRoot(List<FieldVector> list){
    final VectorSchemaRoot vectorSchemaRoot = new VectorSchemaRoot(list);
    vectorSchemaRoot.setRowCount(rowCount);
    return vectorSchemaRoot;
  }

  public void vectorSchemaRootToACsvFileTransform(VectorSchemaRoot vectorSchemaRoot) {
    VarCharVector nameValuesFromVectorSchema = (VarCharVector) vectorSchemaRoot.getVector(0);
    VarCharVector teamValuesFromVectorSchema = (VarCharVector) vectorSchemaRoot.getVector(1);
    VarCharVector positionValuesFromVectorSchema = (VarCharVector) vectorSchemaRoot.getVector(2);
    VarCharVector heightValuesFromVectorSchema = (VarCharVector) vectorSchemaRoot.getVector(3);
    VarCharVector weightValuesFromVectorSchema = (VarCharVector) vectorSchemaRoot.getVector(4);
    VarCharVector ageValuesFromVectorSchema = (VarCharVector) vectorSchemaRoot.getVector(5);
    int aux = 0;
    try {
      FileWriter csvWriter = new FileWriter("mlb_players_result.csv");
      while (aux < rowCount) {
        csvWriter.append(nameValuesFromVectorSchema.getObject(aux).toString());
        csvWriter.append(",");
        csvWriter.append(teamValuesFromVectorSchema.getObject(aux).toString());
        csvWriter.append(",");
        csvWriter.append(positionValuesFromVectorSchema.getObject(aux).toString());
        csvWriter.append(",");
        csvWriter.append(heightValuesFromVectorSchema.getObject(aux).toString());
        csvWriter.append(",");
        csvWriter.append(weightValuesFromVectorSchema.getObject(aux).toString());
        csvWriter.append(",");
        csvWriter.append(ageValuesFromVectorSchema.getObject(aux).toString());
        csvWriter.append("\n");
        ++aux;
      }
      csvWriter.flush();
      csvWriter.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}