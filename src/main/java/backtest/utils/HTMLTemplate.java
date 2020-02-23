package backtest.utils;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Scanner;

public class HTMLTemplate {
  private String html;

  public HTMLTemplate(String filename) {
    ClassLoader classLoader = getClass().getClassLoader();
    Scanner sc = new Scanner(classLoader.getResourceAsStream(filename));
    sc.useDelimiter("\\A");
    html = sc.hasNext() ? sc.next() : "";
  }

  public void replace(String var, String content) {
    html = html.replace("{" + var + "}", content);
  }

  public void save(String output) {
    try {
      PrintWriter pw = new PrintWriter(output);
      pw.print(html);
      pw.close();
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
  }

  public String toString() {
    return html;
  }
}
