package backtest.at;

import java.time.LocalDate;

public class OHLC {
  public String ticker;
  public double open, high, low, close;
  long volume;
  LocalDate date;

  @Override
  public String toString() {
    return String.format("%s date: %s, open: %f, high: %f, low: %f, close: %f, volume: %d", ticker, date, open, high, low, close, volume);
  }
}
