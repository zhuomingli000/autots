package backtest.regime;

import backtest.quant.Portfolio;
import backtest.quant.Stock;
import backtest.quant.Stocks;
import backtest.struct.NumMap;
import backtest.utils.Cal;
import backtest.utils.Util;

import java.time.LocalDate;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class BearFinder {
  public static void main(String[] args) {
    Stocks stocks = new Stocks();
    LocalDate from = LocalDate.of(2007, 1, 1);
    LocalDate to = LocalDate.of(2016, 1, 22);
    stocks.updateTo(to, "SPY");
    Stock spy = stocks.getStockFromCache("SPY");
    double bestSharpe = -1;
    Portfolio bestPortfolio = null;
    double bestThreshold = -1;
    int bestPeriod = -1;
    List<LocalDate> bestList = null;
    for (double threshold = 0.08; threshold < 0.2; threshold += 0.01) {
      for (int period = 5; period <= 60; period += 1) {
        List<LocalDate> bearList = spy.getBearDates(from, to, threshold, period);
        if (bearList.size() < 10 || bearList.get(bearList.size() - 1).isBefore(LocalDate.of(2015, 1, 1))) continue;
        Set<LocalDate> bearSet = new HashSet<>(bearList);
        Portfolio portfolio = new Portfolio(10000, stocks);
        for (LocalDate date = Cal.getLatestBusinessDayAfter(from); date.isBefore(to);
             date = Cal.getNextBusinessDay(date)) {
          if (bearSet.contains(date)) {
            portfolio.reassign(date, new NumMap<String>().put("SPY", 0));
          } else portfolio.reassign(date, new NumMap<String>().put("SPY", 1));
        }
        double sharpeRatio = portfolio.sharpeRatio();
        if (sharpeRatio > 0 && sharpeRatio > bestSharpe) {
          bestSharpe = sharpeRatio;
          bestPortfolio = portfolio;
          bestPeriod = period;
          bestThreshold = threshold;
          bestList = bearList;
        }
      }
    }
    if (bestPortfolio != null) {
      bestPortfolio.plotWithSPY();
      System.out.println("threshold: " + bestThreshold);
      System.out.println("period: " + bestPeriod);
      Util.printColl(bestList);
      System.out.println("total: " + bestList.size());
    }
  }
}
