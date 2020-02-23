package backtest.regime;

import backtest.quant.Portfolio;
import backtest.quant.Stock;
import backtest.quant.Stocks;
import backtest.struct.NumMap;
import backtest.struct.TimeSeries;
import backtest.utils.Cal;
import backtest.utils.Canvas;

import java.time.LocalDate;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SimpleRegimeTest {
  public static void main(String[] args) {
    Stocks stocks = new Stocks();
    stocks.cacheAllStockQueries();
    stocks.updateSP500AndSPYTo(LocalDate.of(2016, 2, 1));
    Stock spy = stocks.getStockFromCache("SPY");
    LocalDate from = LocalDate.of(2009, 4, 1);
    LocalDate to = LocalDate.of(2016, 1, 22);
    double bestsr = -1;
    Portfolio bestp = null;
    int bestper = -2;
    double bestt = -1;
    int bestex = -1;
    for (int period = 5; period < 60; period++) {
      for (double thresh = 0.01; thresh < 0.15; thresh += 0.01) {
        List<LocalDate> bearList = spy.getBearDates(from, to, thresh, period);
        for (int expand = 0; expand < 10; expand++) {
          Set<LocalDate> bearSet = new HashSet<>(bearList);
          for (LocalDate d : bearList) {
            LocalDate next = d;
            for (int i = 0; i < expand; i++) {
              next = Cal.getNextBusinessDay(next);
              bearSet.add(next);
            }
          }
          Portfolio portfolio = new Portfolio(10000, stocks);
          for (LocalDate d = Cal.getLatestBusinessDayAfter(from); d.isBefore(to); d = Cal.getNextBusinessDay(d)) {
            if (bearSet.contains(d)) portfolio.reassign(d, new NumMap<String>().put("SPY", 0));
            else portfolio.reassign(d, new NumMap<String>().put("SPY", 1));
          }
          double sr = portfolio.sharpeRatio();
          if (sr > bestsr) {
            bestsr = sr;
            bestp = portfolio;
            bestper = period;
            bestt = thresh;
            bestex = expand;
          }
        }
      }
    }
    if (bestsr > 0) {
      System.out.println("best sr: " + bestsr);
      System.out.println("best period: " + bestper);
      System.out.println("best thresh: " + bestt);
      System.out.println("best expand: " + bestex);
      Canvas canvas = new Canvas("output/perf.html");
      TimeSeries cumRet = bestp.cumRet();
      canvas.addSeries(stocks.getStockFromCache("SPY").alignWith(cumRet).normalize().setName("SPY"));
      canvas.addSeries(cumRet.setName("portfolio"));
      canvas.draw();
    }
  }
}
