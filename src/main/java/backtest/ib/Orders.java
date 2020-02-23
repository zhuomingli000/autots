package backtest.ib;

import backtest.utils.Util;

import java.time.ZonedDateTime;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class Orders {
  private ConcurrentNavigableMap<ZonedDateTime, FilledState> records;

  public Orders() {
    records = new ConcurrentSkipListMap<>();
  }
  public void record(FilledState filledState) {
    records.put(Util.getEstNow(), filledState);
  }
  public String getHistory() {
    if (records.isEmpty()) return "";
    String str = "Order history";
    for (ConcurrentNavigableMap.Entry<ZonedDateTime, FilledState> entry : records.descendingMap().entrySet()) {
      str += "\n";
      str += entry.getKey() + "\n";
      str += entry.getValue() + "\n";
    }
    return str;
  }
}
