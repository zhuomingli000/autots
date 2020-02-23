package backtest.io;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;

/**
 * sort rows of csv files from yahoo finance
 */
public class YahooDataCmp implements Comparator<ArrayList<String>> {
  private Map<String, Integer> c2i;

  public YahooDataCmp(Map<String, Integer> c2i) {
    this.c2i = c2i;
  }

  @Override
  public int compare(ArrayList<String> o1, ArrayList<String> o2) {
    int di = c2i.get("Date");
    LocalDate d1 = LocalDate.parse(o1.get(di));
    LocalDate d2 = LocalDate.parse(o2.get(di));
    return d1.compareTo(d2);
  }
}
