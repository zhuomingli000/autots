package backtest.at;

import java.time.LocalDateTime;

public final class Gap implements Comparable<Gap> {
  public String ticker;
  public double gap, openPrice, lastClose;
  public LocalDateTime openTime;

  @Override
  public int compareTo(Gap t) {
    return Double.compare(gap, t.gap);
  }
}
