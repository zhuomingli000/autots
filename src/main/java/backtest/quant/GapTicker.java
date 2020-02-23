package backtest.quant;

public class GapTicker implements Comparable<GapTicker> {
  public String ticker;
  public double gap, startPrice, endPrice;
  public int shares;

  public GapTicker(String ticker, double gap, double startPrice, double endPrice) {
    this.ticker = ticker;
    this.gap = gap;
    this.startPrice = startPrice;
    this.endPrice = endPrice;
  }

  @Override
  public int compareTo(GapTicker o) {
    if (Math.abs(gap - o.gap) < 1e-8) return 0;
    return gap < o.gap ? -1 : 1;
  }

  @Override
  public String toString(){
    return "ticker: " + ticker + " gap: " + gap + " shares: " + shares + " startPrice: " + startPrice;
  }
}
