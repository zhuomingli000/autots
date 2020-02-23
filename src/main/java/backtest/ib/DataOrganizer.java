package backtest.ib;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import backtest.quant.Stocks;
import backtest.utils.Cal;

public class DataOrganizer {

  public static void main(String[] args) throws IOException {
    long start = System.currentTimeMillis();
    HashMap<String, Integer> map = new HashMap<String, Integer>();
    BufferedReader br = new BufferedReader(new FileReader("missing"));
    String line;
    while ((line = br.readLine()) != null) {
      String[] tokens = line.split("\\s+");
//      if (!isValidResult(tokens)) {
//        System.out.println("invalid result caught: " + line);
//        continue;
//      }
      String tokenComb = tokens[0] + " " + tokens[1].substring(0, 7);
      //map.put(tokenComb, new DataOrganizer().new Data(tokens[0], tokens[1], tokens[3],  tokens[4],  tokens[5],  tokens[6],  tokens[7]));
      if (!map.containsKey(tokenComb)) {
        System.out.println("no records for "+ tokenComb);
        String output = tokenComb + "\n";
        Files.write(Paths.get("missingMonth"), output.getBytes(), StandardOpenOption.APPEND);
      }
      map.put(tokenComb, 1);
    }
    System.exit(0);
    LocalDate date =Cal.getNextBusinessDay(LocalDate.of(2013, 1, 1));
    Stocks stocks = new Stocks();
    while (date.isBefore(LocalDate.of(2016, 1, 1))) {
      for (String ticker : stocks.getSP500()) {
        if (!map.containsKey(ticker + " " + date.toString())) {
          System.out.println("no records for "+ ticker + " " + date.toString());
          String output = ticker + " " + date.toString() + "\n";
          Files.write(Paths.get("missing"), output.getBytes(), StandardOpenOption.APPEND);
        }
      }
      date = Cal.getNextBusinessDay(date);
    }
    System.out.println("elapsed time " + TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - start));
  }

  private static boolean isValidResult(String[] tokens) {
    if (tokens.length < 8) return false;
    return true;
  }
}
