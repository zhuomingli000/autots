package backtest.at;

import java.time.LocalDateTime;

public class Quote {
  String ticker;
  public LocalDateTime time;
  public double bidPrice, askPrice;
  long bidSize, askSize;
}
