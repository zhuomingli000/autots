package backtest.ib;

import com.ib.client.Contract;

public class FilledState {
  public Contract contract;
  public double filledPrice;
  public long filledShares;
  public int orderId;
  public String orderName;
  public String toString() {
    String str = "Filled: " + contract.description() + "\n";
    str += "Avg filled price: " + filledPrice + "\n";
    str += "Num of shares: " + filledShares + "\n";
    str += "Order id: " + orderId + "\n";
    if (!orderName.isEmpty()) str += "Order name: " + orderName;
    return str;
  }
}
