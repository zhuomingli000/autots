package backtest.quant;

import backtest.struct.ArrayMap;
import backtest.struct.TimeSeries;
import backtest.utils.Cal;
import backtest.utils.Canvas;
import backtest.utils.Util;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Ordering;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

class Record {
  public double lastYearAvgRet = -1000;
  public double todayRet = 0;
  public double gap = -1000;
  public double startPrice = 0;
  public double endPrice = 0;
}

public class GapByExpRet {
  private static final double stopLoss = 0.01;
  private static final double stopWin = 0.08;

  public static void main(String[] args) {
    Stocks stocks = new Stocks();
    stocks.cacheAllStockQueries();
    ArrayMap<LocalDate, List<Record>> timeFrame = new ArrayMap<>();
    LocalDate from = Cal.getLatestBusinessDayAfter(LocalDate.of(2010, 1, 1));
    LocalDate to = Cal.getLatestBusinessDayBefore(LocalDate.of(2016, 4, 29));
    stocks.updateSP500AndSPYTo(to);
    stocks.load(stocks.getSP500AndSPY());
    List<String> sp500 = stocks.getSP500();
    System.out.println("size of sp500: " + sp500.size());
    Stock spy = stocks.getStockFromCache("SPY");
    int spyi = spy.getI(from);
    if (spyi < 1) {
      System.out.println("spyi of from date less than 1");
      return;
    }
    int windowStart = 0;
    List<Double> sum = new ArrayList<>();
    List<Integer> count = new ArrayList<>();
    for (int i = 0; i < sp500.size(); i++) {
      sum.add(0d);
      count.add(0);
    }
    System.out.println("build time frame");
    for (LocalDate date = from; date.isBefore(to); date = Cal.getNextBusinessDay(date)) {
      Util.printAtSameLine(date.toString());
      List<Record> recordList = new ArrayList<>();
      spyi = spy.findFrom(date, spyi);
      if (spyi < 1) {
        System.out.println("can't find spyi");
        return;
      }
      // calc new window start
      int newWindowStart = windowStart;
      while (newWindowStart < timeFrame.size()
          && timeFrame.getKey(newWindowStart).isBefore(date.minusYears(1))) newWindowStart++;
      for (int stockIndex = 0; stockIndex < sp500.size(); stockIndex++) {
        Stock stock = stocks.getStockFromCache(sp500.get(stockIndex));
        Record record = new Record();
        int dateI = stock.getI(date);
        if (dateI < 1) {
          recordList.add(record);
          continue;
        }
        double gap = stock.getGap(dateI) - spy.getGap(spyi);
        record.gap = gap;
        if (newWindowStart != windowStart) {
          // move window forward
          // remove records that are out of window
          // j is the index of the date that is out of window
          for (int dateIndex = windowStart; dateIndex < newWindowStart; dateIndex++) {
            Record recordToBeRemoved = timeFrame.getValue(dateIndex).get(stockIndex);
            if (recordToBeRemoved.gap > 0) {
              sum.set(stockIndex, sum.get(stockIndex) - recordToBeRemoved.todayRet);
              count.set(stockIndex, count.get(stockIndex) - 1);
            }
          }
        }
        if (gap > 0) {
          record.startPrice = stock.getAdjOpen(dateI);
          if ((stock.getAdjOpen(dateI) - stock.getAdjLow(dateI)) / stock.getAdjOpen(dateI)
              > stopLoss) {
            record.endPrice = stock.getAdjOpen(dateI) * (1 - stopLoss);
          }  else if ((stock.getAdjHigh(dateI) - stock.getAdjOpen(dateI))/stock.getAdjOpen(dateI) > stopWin) {
            record.endPrice = stock.getAdjOpen(dateI) * (1 + stopWin);
          } else {
            record.endPrice = stock.getAdjClose(dateI);
          }
          record.endPrice = stock.getAdjClose(dateI);
          record.todayRet = (record.endPrice - record.startPrice) / record.startPrice;
          int c = count.get(stockIndex);
          record.lastYearAvgRet = c == 0 ? 0 : sum.get(stockIndex) / c;
          // add new record into window
          sum.set(stockIndex, sum.get(stockIndex) + record.todayRet);
          count.set(stockIndex, count.get(stockIndex) + 1);
        }
        recordList.add(record);
      }
      // update window start
      windowStart = newWindowStart;
      if (recordList.size() != sp500.size()) {
        System.out.println("size of record list: " + recordList.size());
      }
      timeFrame.put(date, recordList);
    }
    List<Integer> stockIndexList = new ArrayList<>();
    for (int i = 0; i < sp500.size(); i++) {
      stockIndexList.add(i);
    }
    double maxFund = 0;
    TimeSeries bestSeries = new TimeSeries();
    int bestN = 0;
    System.out.println("\nsearch best param");
    for (int n = 6; n < 10; n+=100) {
      Util.printAtSameLine(Integer.toString(n));
      double fund = 30000;
      TimeSeries valSeries = new TimeSeries();
      // TODO(yukang): init date index should be 1 year later instead of 0
      for (int dateIndex = 0; dateIndex < timeFrame.size(); dateIndex++) {
        if (!timeFrame.getKey(dateIndex).isAfter(from.plusYears(1))) continue;
        List<Record> recordList = timeFrame.getValue(dateIndex);
        // stock index comparator
        Comparator<Integer> comparator =
            (o1, o2) ->
                Double.compare(
//                    recordList.get(o1).lastYearAvgRet, recordList.get(o2).lastYearAvgRet
                    recordList.get(o1).gap, recordList.get(o2).gap
                );
        Iterable<Integer> gapTickers =
            FluentIterable.from(stockIndexList)
                .filter(i -> recordList.get(i).gap > 0
//                    && recordList.get(i).lastYearAvgRet > 0
                );
        List<Integer> selected = Ordering.from(comparator).greatestOf(gapTickers, n);
        int numSelected = selected.size();
        double moneyForEach = fund / numSelected;
        System.out.println();
        System.out.println(timeFrame.getKey(dateIndex));
        for (int stockIndex : selected) {
          Record record = recordList.get(stockIndex);
          int numOfShare = (int) (moneyForEach / record.startPrice);
          if (numOfShare > 0) {
            fund -= 2;
            numOfShare = Math.min(200, numOfShare);
            fund += numOfShare * (record.endPrice - record.startPrice);
          }
          System.out.println("select " + sp500.get(stockIndex));
          System.out.println("  gap: " + record.gap);
          System.out.println("  num of share: " + numOfShare);
          System.out.println("  start: " + record.startPrice);
          System.out.println("  end: " + record.endPrice);
          System.out.println("  ret: " + numOfShare * (record.endPrice - record.startPrice));
          System.out.println("  avg ret: " + record.lastYearAvgRet);
        }
        if (valSeries.size()!=0) {
          System.out.println("total: " + (fund-valSeries.lastValue().toDouble()));
        }
        valSeries.put(timeFrame.getKey(dateIndex), fund);
      }
      if (fund > maxFund) {
        maxFund = fund;
        bestSeries = valSeries;
        bestN = n;
      }
    }
    System.out.println();
    System.out.println("max fund: " + maxFund);
    System.out.println("bestN: " + bestN);
    Canvas canvas = new Canvas("output/perf.html");
    canvas.addSeries(stocks.getStockFromDB("SPY").alignWith(bestSeries).normalize().setName("SPY"));
    canvas.addSeries(bestSeries.normalize());
    canvas.draw();
  }
}
