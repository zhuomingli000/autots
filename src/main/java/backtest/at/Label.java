package backtest.at;

import java.time.LocalDateTime;

public final class Label {
  String ticker;
  LocalDateTime from;
  LocalDateTime to;
  @Override
  public String toString() {
    return ticker + " from: " + from + " to: " + to;
  }
}
