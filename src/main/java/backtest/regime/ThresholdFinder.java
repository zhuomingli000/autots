package backtest.regime;

import backtest.quant.Portfolio;
import backtest.quant.Stocks;
import backtest.struct.NumMap;
import backtest.struct.TimeSeries;
import backtest.utils.Cal;
import backtest.utils.Canvas;
import backtest.utils.StopWatch;

import java.time.LocalDate;
import java.util.PriorityQueue;

public class ThresholdFinder {
  private final static Stocks stocks = new Stocks();
  private final static StopWatch sw = new StopWatch();

  public void test() {
//        update symbols if last update is weeks ago.
    stocks.updateSP500AndSPYTo(LocalDate.of(2016, 1, 25));
    stocks.cacheAllStockQueries();
    LocalDate startDay = LocalDate.of(2005, 1, 1);
    LocalDate realStartDay = LocalDate.of(2010, 1, 1);
    LocalDate endDay = LocalDate.of(2016, 1, 10);
    PriorityQueue<RegimeModel> pq = new PriorityQueue<>();
    double bestSharpeRatio = -1;
    Portfolio bestPortfolio = null;
    TimeSeries highMinusLow = null;
    TimeSeries high = null;
    TimeSeries low = null;
    for (int extremeWindowSize = 37; extremeWindowSize < 38; extremeWindowSize++) {
      System.out.println("extreme win size: " + extremeWindowSize);
      sw.start();
      RegimeModel model = new RegimeModel(stocks);
      model.calcHighMinusLowRate(extremeWindowSize, startDay, endDay);
      sw.stop();
      sw.start();
      final double thresholdIncRate = 1.0 / 500;
      for (double threshold = 0; threshold < 0.005; threshold += thresholdIncRate) {
        model.findBearDays(threshold);
        if (model.tooFewBearDays() || model.onlyBefore2010()) break;
        Portfolio portfolio = new Portfolio(10000, stocks);
        for (LocalDate date = Cal.getLatestBusinessDayAfter(realStartDay); date.isBefore(endDay);
             date = Cal.getNextBusinessDay(date)) {
          if (model.isBear(date)) {
            portfolio.reassign(date, new NumMap<String>().put("SPY", 0));
          } else portfolio.reassign(date, new NumMap<String>().put("SPY", 1));
        }
        model.sharpeRatio = portfolio.sharpeRatio();
        if (model.sharpeRatio < 0) continue;
        if (model.sharpeRatio > bestSharpeRatio) {
          bestSharpeRatio = model.sharpeRatio;
          bestPortfolio = portfolio;
          highMinusLow = model.highMinusLow;
          high = model.high.setName("high");
          low = model.low.setName("low");
        }
        model.mean = portfolio.mean();
        model.std = portfolio.std();
        pq.add(new RegimeModel(model));
        if (pq.size() > 20) pq.poll();
      }
      sw.stop();
    }
    System.out.println("pq size: " + pq.size());
    while (!pq.isEmpty()) {
      System.out.println(pq.poll() + "\n");
    }
    if (bestSharpeRatio > 0) {
      Canvas canvas = new Canvas("output/perf.html");
      TimeSeries cumRet = bestPortfolio.cumRet();
      canvas.addSeries(stocks.getStockFromCache("SPY").alignWith(cumRet).normalize().setName("SPY"));
      canvas.addSeries(cumRet.setName("portfolio"));
      canvas.addSeries(highMinusLow, 1);
      canvas.addSeries(high, 2);
      canvas.addSeries(low, 2);
      canvas.draw();
    }
  }

  public static void main(String[] args) {
    new ThresholdFinder().test();
  }
}