package backtest.quant;

import backtest.regime.RegimeModel;
import backtest.struct.NumMap;
import backtest.struct.TimeSeries;
import backtest.utils.Cal;
import backtest.utils.Canvas;
import backtest.utils.Num;
import backtest.utils.StopWatch;
import com.google.common.collect.Ordering;
import com.google.common.collect.Table;
import com.google.common.collect.TreeBasedTable;

import java.time.LocalDate;
import java.util.*;

public class BuyOnGap {
  private final Stocks stocks;
  private final List<String> sp500;
  private final RegimeModel regimeModel;
  private final Stock spy;

  public BuyOnGap(Stocks stocks) {
    this.stocks = stocks;
    this.sp500 = stocks.getSP500();
    this.regimeModel = new RegimeModel(stocks);
    this.regimeModel.calcHighMinusLowRate(12, LocalDate.of(2006,1,1), LocalDate.of(2016,4,8));
    this.spy = stocks.getStockFromCache("SPY");
  }

  public static final class Params {
    int bearPeriod, n, useHighNum;
    double bearThresh, thresh, ub, sw, stopLoss;

    Params(int bearPeriod, double bearThresh, double thresh, double ub, double sw, int n, int useHighNum, double stopLoss) {
      this.bearPeriod = bearPeriod;
      this.bearThresh = bearThresh;
      this.thresh = thresh;
      this.ub = ub;
      this.sw = sw;
      this.n = n;
      this.useHighNum = useHighNum;
      this.stopLoss = stopLoss;
    }

    public String toString() {
      return "bear period " + bearPeriod + "\nbear thresh " + bearThresh + "\nbest thresh "
          + thresh + "\nbest upper bound " + ub + "\nstop win: " + sw + " max # of orders: " + n + " stop loss: " + stopLoss;
    }
  }

  class Result {
    double total;
    Table<LocalDate, LocalDate, Double> retTable;
    TimeSeries valSeries;
    NumMap.Drawdown drawdown;
    public Result(double total, Table<LocalDate, LocalDate, Double> retTable, TimeSeries valSeries) {
      this.total = total;
      this.retTable = retTable;
      this.valSeries = valSeries;
      this.drawdown = valSeries.maxDrawdown();
    }
  }

  enum Strategy {
    PARTIAL_GAP_DOWN,
    PARTIAL_GAP_UP,
    PARTIAL_GAP_DOWN_ADJUSTED,
    FULL_GAP_DOWN,
    FULL_GAP_DOWN_ADJUSTED
  }

  public Result runWith(double total, Params p, LocalDate from, LocalDate to, Strategy strategy) {
    // calc portfolio value
    from = Cal.getLatestBusinessDayAfter(from);
    to = Cal.getLatestBusinessDayBefore(to);
    Map<String, Integer> index = new HashMap<>();
    int spyi = spy.getI(from);
    Table<LocalDate, LocalDate, Double> retTable = TreeBasedTable.create();
    double prev = total;
    LocalDate prevDate = from;
    TimeSeries valSeries = new TimeSeries();

    // iterate all days
    for (LocalDate date = from; date.isBefore(to); date = Cal.getNextBusinessDay(date)) {

      if (p.useHighNum==1) {
        // if no stock reaches 52 week high yesterday
        double hn = regimeModel.high.get(Cal.getPrevBusinessDay(date));
        if (Double.isNaN(hn) || Math.abs(hn) < 1e-3) continue;
      }

      // check market return
      int newSpyi = spy.findFrom(date, spyi);
      if (newSpyi < 1) {
        System.out.println("can't find spyi");
        continue;
      } else {
        spyi = newSpyi;
      }
//      double spyRet = spy.getRetTo(spyi - 1, p.bearPeriod);
//      if (Double.isNaN(spyRet)) continue;
//      if (spyRet < -p.bearThresh) continue;
      List<GapTicker> listTickers = new ArrayList<>();
      // iterate all stocks
      for (String ticker : sp500) {
        // calc gap
        Stock stock = stocks.getStockFromCache(ticker);
        int i = stock.findFrom(date, index.getOrDefault(ticker, 0));
        if (i < 1) continue;
        index.put(ticker, i);
        double gap;
        if (Strategy.PARTIAL_GAP_DOWN_ADJUSTED == strategy) {
          gap = stock.getGap(i) - spy.getGap(spyi);
        } else if (Strategy.PARTIAL_GAP_DOWN == strategy) {
          gap = stock.getGap(i);
        } else if (Strategy.FULL_GAP_DOWN == strategy) {
          gap = stock.getFullGap(i);
        } else if (Strategy.FULL_GAP_DOWN_ADJUSTED == strategy) {
          gap = stock.getFullGap(i) - spy.getFullGap(spyi);
        } else {
          throw new IllegalArgumentException("strategy not implementd");
        }
        if (Double.isNaN(gap) || gap < p.thresh || gap > p.ub) continue;

        // determine endPrice by stopLoss and stopWin
        double high = stock.getAdjHigh(i);
        double low = stock.getAdjLow(i);
        double adjOpen = stock.getAdjOpen(i);
        double endPrice;
        if ((high - adjOpen) / adjOpen > p.sw) endPrice = adjOpen * (1 + p.sw);
        else if ((adjOpen - low) / adjOpen > p.stopLoss) {
          endPrice = adjOpen * (1 - p.stopLoss);
        } else endPrice = stock.getAdjClose(i);
        listTickers.add(new GapTicker(ticker, gap, stock.getAdjOpen(i), endPrice));
//        System.out.println(date + ": " + ticker + " gap: " + gap + " open: " + stock.getAdjOpen(i));
      }

      // calc strategy return
      listTickers = Ordering.natural().greatestOf(listTickers, p.n);
      int numStocks = listTickers.size();
      System.out.println();
      for (GapTicker stock : listTickers) {
        double moneyForEach = total / numStocks;
        int numOfShare = (int) (moneyForEach / stock.startPrice);
        if (numOfShare > 0) {
          total -= 2;
          numOfShare = Math.min(200, numOfShare);
          total += numOfShare * (stock.endPrice - stock.startPrice);
          System.out.println(date + ": " + stock.ticker);
          System.out.println("  num of share: " + numOfShare);
          System.out.println("  start: " + stock.startPrice);
          System.out.println("  end: " + stock.endPrice);
          System.out.println("  ret+2: " + numOfShare * (stock.endPrice - stock.startPrice));
        }
      }
      if (valSeries.size()!=0) {
        System.out.println(date + " total ret: " + (total - valSeries.lastValue().toDouble()));
      }
      valSeries.put(date, total);
      if (!date.isBefore(prevDate.plusMonths(11))) {
        double yearRet = (total - prev) / prev;
        retTable.put(prevDate, date, yearRet);
        prevDate = date;
        prev = total;
      }
    }
    return new Result(total, retTable, valSeries);
  }

  public Params findParam(LocalDate from, LocalDate to, Strategy strategy) {
    System.out.println("findParam: from: " + from + " to: " + to);
    // set up search space.
    // params: bearPeriod int , bearThresh double, thresh double, upper bound double, stopWin double
    List<Double> stopWins = new ArrayList<>();
    stopWins.add(0.08);
//    stopWins.add(1d);
//    for (double s = 0.01; s < 0.09; s += 0.01) stopWins.add(s);
    List<Double> bearThreshs = new ArrayList<>();
    bearThreshs.add(1d);
//    for (double t = 0.01; t < 0.05; t += 0.01) bearThreshs.add(t);
    double bestTotal = 0;
    Params bestParams = null;
    Table<LocalDate, LocalDate, Double> bestRet = null;
    TimeSeries bestSeries = null;
    TimeSeries bestRetSeries = null;
    int bestUseHigh = -1;

    // iterate all cases.
    for (double thresh = 0; thresh < 0.04; thresh += 0.01) {
      System.out.println("thresh: " + thresh);
      StopWatch watch = new StopWatch();
      watch.start();
      List<Double> ubs = new ArrayList<>();
      ubs.add(1d);
      for (double ub = thresh + 0.03; ub < thresh + 0.1; ub += 0.01) ubs.add(ub);
      for (double ub : ubs) {
        for (int bearPeriod = 5; bearPeriod < 25; bearPeriod += 500) {
          for (double bearThresh : bearThreshs) {
            for (double sw : stopWins) {
              for (int n = 1; n < 20; n+=1) {
                for (int useHighNum = 0; useHighNum < 2; useHighNum+=100) {
                  for (double stopLoss = 0.01; stopLoss < 0.05; stopLoss+=0.01) {
                    Params p = new Params(bearPeriod, bearThresh, thresh, ub, sw, n, useHighNum, stopLoss);
                    double initTotal = 30000;
                    Result r = runWith(initTotal, p, from, to, strategy);
                    TimeSeries retSeries = r.valSeries.getRet(1);
                    double totalRet = r.total / initTotal - 1;
                    if (totalRet - r.drawdown.drawdown > bestTotal) {
                      bestTotal = totalRet - r.drawdown.drawdown;
                      bestParams = p;
                      bestRet = r.retTable;
                      bestSeries = r.valSeries;
                      bestRetSeries = retSeries;
                      bestUseHigh = useHighNum;
                    }
                  }
                }
              }
            }
          }
        }
      }
      watch.stop();
    }
    if (bestParams != null) {
      System.out.println("from: " + from);
      System.out.println("to: " + to);
      System.out.println("best total: " + bestTotal);
      System.out.println("best daily mean: " + bestRetSeries.mean());
      System.out.println("best daily std: " + bestRetSeries.std());
      System.out.println("best param:\n" + bestParams);
      System.out.println("total: " + bestTotal);
      System.out.println("best use high: " + bestUseHigh);
      System.out.println(bestSeries.maxDrawdown());
//      for (Table.Cell<LocalDate, LocalDate, Double> cell : bestRet.cellSet()) {
//        LocalDate tfrom = cell.getRowKey();
//        LocalDate tto = cell.getColumnKey();
//        System.out.println("ret from " + tfrom + " to " + tto + ": " + cell.getValue());
//        double spyRet = spy.getRet(tfrom, tto);
//        if (Double.isNaN(spyRet)) {
//          System.out.println("couldn't find spy return");
//        } else {
//          System.out.println("spy: " + spyRet);
//        }
//      }
    } else {
      System.out.println("no param found.");
    }
    return bestParams;
  }

  public static void main(String[] args) {
    // update stock data.
    Stocks stocks = new Stocks();
    stocks.cacheAllStockQueries();
//    LocalDate to = LocalDate.of(2016,4,28);
    LocalDate to = LocalDate.now();
    stocks.updateSP500AndSPYTo(to);
    stocks.load(stocks.getSP500AndSPY());
    BuyOnGap buyOnGap = new BuyOnGap(stocks);
    final Strategy strategy = Strategy.PARTIAL_GAP_DOWN_ADJUSTED;
//    LocalDate from = LocalDate.of(2011,5,1);
//    LocalDate to = LocalDate.of(2015,1,1);
//    for (; from.isBefore(to); from = from.plusMonths(12)) {
//      Params p = buyOnGap.findParam(from, from.plusYears(1), strategy);
//      LocalDate runFrom = from.plusYears(1);
//      LocalDate runTo = runFrom.plusMonths(12);
//      if (runTo.isAfter(LocalDate.now())) runTo = LocalDate.now();
//      double initTotal = 30000;
//      Result r = buyOnGap.runWith(initTotal, p, runFrom, runTo, strategy);
//      System.out.println("result: ");
//      System.out.println("from: " + runFrom);
//      System.out.println("to: " + runTo);
//      System.out.println("total: " + r.total);
//      System.out.println("ret: " + (r.total/initTotal-1));
//      System.out.println("max drawdown: " + r.drawdown);
//      System.out.println("spy ret: " + stocks.getStockFromCache("SPY").getRet(runFrom, runTo) + "\n");
//    }
//    Params p = buyOnGap.findParam(LocalDate.now().minusYears(1), LocalDate.now(), strategy);
//    Result result = buyOnGap.runWith(30000, p, LocalDate.now().minusYears(1), LocalDate.now(), strategy);
    Result result = buyOnGap.runWith(30000, new Params(5,1,0,1,0.08,6,0,0.01), to.minusYears(1), to, strategy);
//    for (Map.Entry<LocalDate, Num> entry : result.valSeries.entrySet()) {
//      System.out.println(entry.getKey() + ": " + entry.getValue().toDouble());
//    }
    Canvas canvas = new Canvas("output/perf.html");
    canvas.addSeries(stocks.getStockFromDB("SPY").alignWith(result.valSeries).normalize().setName("SPY"));
    canvas.addSeries(result.valSeries.normalize());
    canvas.draw();
  }
}
