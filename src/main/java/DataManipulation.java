import static java.util.Arrays.asList;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;


public class DataManipulation {
  public static final RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);

  final VarCharVector nameVector = new VarCharVector("name", allocator);
  final VarCharVector teamVector = new VarCharVector("team", allocator);
  final VarCharVector positionVector = new VarCharVector("position",allocator);
  final VarCharVector heightVector = new VarCharVector("height", allocator);
  final VarCharVector weightVector = new VarCharVector("weight", allocator);
  final VarCharVector  ageVector = new VarCharVector("age", allocator);

  public void readCSVFile(String path) throws FileNotFoundException {
    String line;
    try {
      BufferedReader br = new BufferedReader(new FileReader(path));
        int i = 0;
        while ((line = br.readLine()) != null) {
              String [] values = line.split(",");
              nameVector.setSafe(i, values[0].getBytes());
              teamVector.setSafe(i, values[1].getBytes());
              positionVector.setSafe(i, values[2].getBytes());
              heightVector.setSafe(i, values[3].getBytes());
              weightVector.setSafe(i, values[4].getBytes());
              ageVector.setSafe(i, values[5].getBytes());
            }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  public void vectorToAVectorSchemaRoot(){
    List<FieldVector> list = asList(nameVector, teamVector,
        positionVector, heightVector, weightVector, ageVector);
    try(final VectorSchemaRoot vectorSchemaRoot = new VectorSchemaRoot(list)) {
      vectorSchemaRoot.setRowCount(1035);
    }
  }
 /* public void imprime() {
    System.out.println("--------Nomes:--------");
    for (String name : nameVector) {
      System.out.println(name);
    }
    System.out.println("--------Team:--------");
    for (String team : teamVector) {
      System.out.println(team);
    }
    System.out.println("--------Position:--------");
    for (String position : positionVector) {
      System.out.println(position);
    }
    System.out.println("--------Height:--------");
    for (String height : heightVector) {
      System.out.println(height);
    }
    System.out.println("--------Weight:--------");
    for (String weight : weightVector) {
      System.out.println(weight);
    }
    System.out.println("--------Age:--------");
    for (String age : ageVector) {
      System.out.println(age);
    }
  }*/
}

