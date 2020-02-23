package backtest.quant;

import backtest.struct.ArrayMap;
import backtest.struct.NumMap;
import backtest.struct.TimeSeries;
import backtest.utils.Cal;
import backtest.utils.Canvas;
import backtest.utils.Num;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;

public class Portfolio {
  private final static Logger logger = LoggerFactory.getLogger(Portfolio.class);
  private Map<String, Integer> shares;
  private ArrayMap<String, Num> weights;
  private TimeSeries valSeries;
  private double initWealth;
  private double remaining;
  private Stocks stocks;

  public Portfolio(double initWealth, Stocks stocks) {
    this.initWealth = initWealth;
    this.remaining = 0;
    this.stocks = stocks;
    // TODO: provide setter of whether cache all stock queries and update symbols before reassign.
    stocks.cacheAllStockQueries();
    this.shares = new HashMap<>();
    this.weights = new NumMap<>();
    this.valSeries = new TimeSeries();
  }

  // TODO: diff weight, generate orders, calc commission fee.
  public void reassign(LocalDate date, ArrayMap<String, Num> newWeights) {

    if (!Cal.isBusinessDay(date)) {
      logger.error("reassign portfolio weight on non business day");
      return;
    }
    double wealth = calcWealth(date);
    valSeries.put(date, wealth);
    // no rebalance if weights haven't changed.
    if (weights.equals(newWeights)) return;
    remaining = wealth;
    for (Map.Entry<String, Num> weight : newWeights.entrySet()) {
      String ticker = weight.getKey();
      if (!stocks.isValid(ticker, date)) {
        logger.error("invalid date {} of {}", ticker, date);
        return;
      }
      double availableFund = wealth * weight.getValue().toDouble();
      Stock stock = stocks.getStockFromCache(ticker);
      int newShares = (int) (availableFund / stock.getAdjClose(date));
      shares.put(ticker, newShares);
      remaining -= newShares * stock.getAdjClose(date);
    }
    weights = newWeights;
  }

  private double calcWealth(LocalDate date) {
    if (shares.size() == 0) return initWealth;
    double wealth = remaining;
    for (Map.Entry<String, Integer> entry : shares.entrySet()) {
      String ticker = entry.getKey();
      if (!stocks.isValid(ticker, date)) {
        logger.error("invalid date {} of {}", ticker, date);
        return 0;
      }
      Stock stock = stocks.getStockFromCache(ticker);
      wealth += entry.getValue() * stock.getAdjClose(date);
    }
    return wealth;
  }

  public TimeSeries ret() {
    TimeSeries series = new TimeSeries();
    double prev = -1;
    for (Map.Entry<LocalDate, Num> entry : valSeries.entrySet()) {
      double cur = entry.getValue().toDouble();
      if (prev > 0) {
        series.put(entry.getKey(), (cur - prev) / prev);
      }
      prev = cur;
    }
    return series;
  }

  // daily
  public double sharpeRatio() {
    TimeSeries ret = ret();
    return ret.mean() / ret.std();
  }

  public double mean() {
    return ret().mean();
  }

  public double std() {
    return ret().std();
  }

  public TimeSeries cumRet() {
    return valSeries.divide(valSeries.firstValue().toDouble());
  }

  public void plotWithSPY() {
    Canvas canvas = new Canvas("output/perf.html");
    TimeSeries cumRet = cumRet();
    canvas.addSeries(stocks.getStockFromCache("SPY").alignWith(cumRet).normalize().setName("SPY"));
    canvas.addSeries(cumRet.setName("portfolio"));
    canvas.draw();
  }
}
