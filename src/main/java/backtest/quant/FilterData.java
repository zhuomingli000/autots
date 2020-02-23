package backtest.quant;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;

public class FilterData {

  public static void main(String args[]) throws IOException {
    HashMap<String, Integer> map = new HashMap<>();
    BufferedReader br = new BufferedReader(new FileReader("data/quotes"));
    String line;
    while ((line = br.readLine()) != null) {
      // e.g. : A 2015-06-22T09:30 39.94 3 39.75 5
      String[] tokens = line.split("\\s+");
      String tokenComb = tokens[0] + " " + tokens[1];
      System.out.println("tokenComb is " + tokenComb);
      if (!map.containsKey(tokenComb)) {
        String output = tokenComb +" " + tokens[2] + " " + tokens[4] + "\n";
        Files.write(Paths.get("data/filter-quotes"), output.getBytes(),
            StandardOpenOption.APPEND);
      }
      map.put(tokenComb, 1);
    }
    br.close();
    System.out
        .println("selecting historical data done. map size " + map.size());
  }
}
