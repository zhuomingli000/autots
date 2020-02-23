package backtest.io;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;

public class CSVWriter {
  private PrintWriter writer;

  /**
   * close method must be called to save file.
   *
   * @param file output path
   * @param h    headers in order
   */
  public CSVWriter(String file, ArrayList<String> h) {
    try {
      writer = new PrintWriter(file);
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
    println(h);
  }

  /**
   * strings are escaped in the same style as CSVReader
   */
  public void println(ArrayList<String> list) {
    String line = "";
    int numCol = list.size();
    for (int i = 0; i < numCol; i++) {
      String str = list.get(i);
      if (str.contains(",") || str.startsWith("\"")) {
        for (int j = 0; j < str.length(); j++) {
          if (str.charAt(j) == '"') {
            str = str.substring(0, j) + "\"\"" + str.substring(j + 1);
            j++;
          }
        }
        str = "\"" + str + "\"";
      }
      if (i == numCol - 1) line += str;
      else line += (str + ",");
    }
    writer.println(line);
  }

  public void close() {
    writer.close();
  }
}
