package backtest.regime;

import backtest.quant.Stock;
import backtest.quant.Stocks;
import backtest.struct.TimeSeries;
import backtest.utils.Num;
import backtest.utils.StopWatch;
import backtest.utils.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.Period;
import java.util.*;
import java.util.concurrent.*;

public class RegimeModel implements Comparable<RegimeModel> {
  private static final Logger logger = LoggerFactory.getLogger(RegimeModel.class);
  private Stocks stocks;

  /**
   * extremeWindowSize size used by finding number of stocks that reach highest or
   * lowest point in a given size window. unit: month. Values: 1
   * month, 2 months, ... 36 months
   */
  public int extremeWindowSize;
  public TimeSeries highMinusLow, high, low;
  public Map<LocalDate, Double> hmap, lmap;
  private List<LocalDate> bearDayList;
  private Set<LocalDate> bearDaySet;
  public double sharpeRatio, mean, std, threshold;

  public RegimeModel(Stocks stocks) {
    this.stocks = stocks;
  }

  public RegimeModel(RegimeModel model) {
    this.extremeWindowSize = model.extremeWindowSize;
    this.mean = model.mean;
    this.sharpeRatio = model.sharpeRatio;
    this.std = model.std;
    this.threshold = model.threshold;
  }

  // TODO: have a builder that gives high, low, high minus low.
  public void calcHighMinusLowRate(int extremeWindowSize, LocalDate startDay, LocalDate endDay) {
    this.extremeWindowSize = extremeWindowSize;
    final ConcurrentMap<LocalDate, Double> highMap = new ConcurrentHashMap<>();
    final ConcurrentMap<LocalDate, Double> lowMap = new ConcurrentHashMap<>();
    List<String> sp500 = stocks.getSP500();
    TimeSeries totalValidStockNumSeries = stocks.getValidStockNumSeries(sp500, startDay, endDay);
    logger.info("start calc high minus low rate");
    sp500.forEach(ticker -> {
      final Stock stock = stocks.getStockFromCache(ticker);
      stock.getExtremeDays(startDay, endDay, Period.ofMonths(extremeWindowSize), true)
          .forEach(d -> highMap.merge(d, 1d, Double::sum));
      stock.getExtremeDays(startDay, endDay, Period.ofMonths(extremeWindowSize), false)
          .forEach(d -> lowMap.merge(d, -1d, Double::sum));
    });
    // 2880
    high = new TimeSeries(highMap).setName("high");
    low = new TimeSeries(lowMap).setName("low");
    hmap = new HashMap<>(highMap);
    lmap = new HashMap<>(lowMap);
    highMinusLow = high.add(low).divide(totalValidStockNumSeries);
  }

  @Override
  public int compareTo(RegimeModel o) {
    return Util.compareDouble(sharpeRatio, o.sharpeRatio, 1e-5);
  }

  public void findBearDays(double threshold) {
    this.threshold = threshold;
    bearDayList = new ArrayList<>();
    for (Map.Entry<LocalDate, Num> entry : highMinusLow.entrySet()) {
      if (entry.getValue().toDouble() < threshold) {
        bearDayList.add(entry.getKey());
      }
    }
    bearDaySet = new HashSet<>(bearDayList);
  }

  public boolean tooFewBearDays() {
    return bearDayList.size() < 10;
  }

  public boolean onlyBefore2010() {
    LocalDate crisisEnd = LocalDate.of(2010, 1, 1);
    return bearDayList.get(bearDayList.size() - 1).isBefore(crisisEnd);
  }

  @Override
  public String toString() {
    return "extreme window size: " + extremeWindowSize +
        "\nsharpe ratio: " + sharpeRatio +
        "\nthreshold: " + threshold +
        "\nmean: " + mean +
        "\nstd: " + std;

  }

  public boolean isBear(LocalDate date) {
    return bearDaySet.contains(date);
  }

  public static void main(String[] args) {
    Stocks stocks = new Stocks();
    stocks.cacheAllStockQueries();
    stocks.load(stocks.getSP500());
    RegimeModel model = new RegimeModel(stocks);
    StopWatch sw = new StopWatch();
    sw.start();
    model.calcHighMinusLowRate(12, LocalDate.of(2013,1,1), LocalDate.of(2016,1,1));
    sw.stop();
  }
}
