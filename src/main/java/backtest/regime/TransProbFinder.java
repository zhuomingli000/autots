package backtest.regime;

import backtest.quant.Stock;
import backtest.quant.Stocks;
import backtest.utils.Cal;

import java.time.LocalDate;
import java.util.HashSet;
import java.util.Set;

public class TransProbFinder {
  public double pBullToBear, pBearToBear, total, bearTotal, bullToBear, bearToBear;

  public TransProbFinder(LocalDate from, LocalDate to) {
    double threshold = 0.08;
    int period = 27;
    Stocks stocks = new Stocks();
    Stock spy = stocks.getStockFromCache("SPY");
    Set<LocalDate> bearSet = new HashSet<>(spy.getBearDates(from, to, threshold, period));
    total = 0;
    bearTotal = bearSet.size();
    bullToBear = 0;
    bearToBear = 0;
    for (LocalDate date = Cal.getLatestBusinessDayAfter(from), next = Cal.getNextBusinessDay(date);
         next.isBefore(to);
         date = Cal.getNextBusinessDay(date), next = Cal.getNextBusinessDay(next)) {
      total++;
      if (bearSet.contains(date) && bearSet.contains(next)) bearToBear++;
      else if (!bearSet.contains(date) && bearSet.contains(next)) bullToBear++;
    }
    pBullToBear = bullToBear / (total - bearTotal);
    pBearToBear = bearToBear / bearTotal;
  }

  public void print() {
    System.out.println("total: " + total);
    System.out.println("bear: " + bearTotal);
    System.out.println("bull to bear #: " + bullToBear);
    System.out.println("bear to bear #: " + bearToBear);
    System.out.println("bull to bull: " + (1 - pBullToBear));
    System.out.println("bull to bear: " + pBullToBear);
    System.out.println("bear to bear: " + pBearToBear);
    System.out.println("bear to bull: " + (1 - pBearToBear));
  }

  public static void main(String[] args) {
    LocalDate from = LocalDate.of(2007, 1, 1);
    LocalDate to = LocalDate.of(2016, 1, 22);
    new TransProbFinder(from, to).print();
  }
}
