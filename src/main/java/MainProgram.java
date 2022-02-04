import java.io.FileNotFoundException;

public class MainProgram {
  public static void main(String[] args) throws FileNotFoundException {
    String path = "/home/gabriel/simbiose/Challenge-01/mlb_players.csv";
    DataManipulation data = new DataManipulation();

    data.readCSVFile(path);
    data.vectorToAVectorSchemaRoot();

  }
}
