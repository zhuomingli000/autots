package backtest.ib;

import com.ib.client.TickType;

public class ResultBundle {
  TickType tickType;
  public double price;
  int canAutoExecute;

  public ResultBundle(TickType tickType, double price, int canAutoExecute) {
    this.tickType = tickType;
    this.price = price;
    this.canAutoExecute = canAutoExecute;
  }

  public String toString() {
    return String.format("tickType %s, price %f, canAutoExecute %d",
        tickType.toString(),
        price,
        canAutoExecute);

  }
}
