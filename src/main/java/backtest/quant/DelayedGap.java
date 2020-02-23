package backtest.quant;

import backtest.struct.Data;
import backtest.struct.TimeSeries;
import backtest.utils.Cal;
import backtest.utils.Canvas;
import backtest.utils.Num;

import com.google.common.collect.Ordering;
import com.google.common.collect.Table;
import com.google.common.collect.TreeBasedTable;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.*;
import java.util.Map.Entry;

public class DelayedGap {
  private final List<String> sp500;

  public DelayedGap(Stocks stocks) {
    this.sp500 = stocks.getSP500();
  }

  public static final class Params {
    int n;
    int secDelay;

    Params(int n, int sec) {
      this.n = n;
      this.secDelay = sec;
    }

    public String toString() {
      return String.format("highest %d; secDelay %d ", n, secDelay);
    }
  }

  class Result {
    double total;
    Table<LocalDate, LocalDate, Double> retTable;
    TimeSeries valSeries;

    public Result(double total, Table<LocalDate, LocalDate, Double> retTable,
        TimeSeries valSeries) {
      this.total = total;
      this.retTable = retTable;
      this.valSeries = valSeries;
    }
  }

  public Result runWith(HashMap<String, Data> dataMap, double total, Params p,
      LocalDate from, LocalDate to) throws IOException {
    // calc portfolio value
    from = Cal.getLatestBusinessDayAfter(from);
    to = Cal.getLatestBusinessDayBefore(to);
    Table<LocalDate, LocalDate, Double> retTable = TreeBasedTable.create();
    double prev = total;
    LocalDate prevDate = from;
    TimeSeries valSeries = new TimeSeries();

    // iterate all days
    for (LocalDate date = from; date.isBefore(to); date = Cal
        .getNextBusinessDay(date)) {
      List<GapTicker> listTickers = new ArrayList<>();
      int noPrevData = 0;
      int noOpenData = 0;
      int no931Data = 0;
      // iterate all stocks
      for (String ticker : sp500) {
        // calc gap
        Data data930 = dataMap.get(ticker + " " + date.toString() + "T09:30");
        if (data930 == null) {
          // System.out.println("no data for 930 key: " + ticker + " "
          // + date.toString() + " 09:30");
          noOpenData++;
          continue;
        }
        double gap;
        Data dataPrevClose = dataMap.get(ticker + " "
            + Cal.getPrevBusinessDay(date) + "T15:59");
        if (dataPrevClose == null) {
          // System.out.println("no data prev on " + ticker + " "
          // + Cal.getPrevBusinessDay(date).toString());
          noPrevData++;
          continue;
        }
        gap = (dataPrevClose.getAsk() - data930.getAsk())
            / dataPrevClose.getAsk();

        Data data931 = dataMap.get(ticker + " " + date.toString() + "T"
            + LocalTime.of(9, 30, 0).plusSeconds(p.secDelay).toString());
        if (data931 == null) {
          data931 = dataMap.get(ticker + " " + date.toString() + "T"
              + LocalTime.of(9, 30, 0).plusSeconds(p.secDelay-1).toString());
        }
        if (data931 == null) {
          no931Data ++;
          continue;
        }
        double adjOpen = data931.getAsk();
        double endPrice;
        Data data359 = dataMap.get(ticker + " " + date.toString() + "T15:59");
        if (data359 == null) {
          System.out.println("no data for 359 " + ticker + " "
              + date.toString());
          continue;
        }
        endPrice = data359.getBid(); // assume sold at the start of 359
        if ((adjOpen - endPrice) / adjOpen > 0.02) {
          endPrice = adjOpen * 0.98;
        }
//        if ((endPrice - adjOpen) / adjOpen > 0.8) {
//          endPrice = adjOpen * 1.8;
//        }
        listTickers.add(new GapTicker(ticker, gap, adjOpen, endPrice));
        // System.out.println(date + ": " + ticker + " gap: " + gap + " open: "
        // + stock.getAdjOpen(i));
      }

      System.out.println("noPrevData : " + noPrevData);
      System.out.println("noOpenData : " + noOpenData);
      System.out.println("no931Data : " + no931Data);

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
          System.out.println("  ret+2: " + numOfShare
              * (stock.endPrice - stock.startPrice));
        }
      }
      if (valSeries.size() != 0) {
        System.out.println(date + " total ret: "
            + (total - valSeries.lastValue().toDouble()));
      }
      valSeries.put(date, total);
      if (!date.isBefore(prevDate.plusMonths(11))) {
        double yearRet = (total - prev) / prev;
        retTable.put(prevDate, date, yearRet);
        prevDate = date;
        prev = total;
      }
    }
    double lastTotal = 30000;
    for (Entry<LocalDate, Num> entry : valSeries.entrySet()) {
      System.out.println(entry.getKey().toString() + " "
          + (entry.getValue().toDouble() - lastTotal));
      lastTotal = entry.getValue().toDouble();
    }

    System.out.println(String.format("total from %s to %s : ", from.toString(),
        to.toString()) + total);
    return new Result(total, retTable, valSeries);
  }

  public static void main(String[] args) throws IOException {
    // update stock data.
    Stocks stocks = new Stocks();
    stocks.cacheAllStockQueries();
    LocalDate to = LocalDate.of(2016, 6, 22);
    DelayedGap buyOnGap = new DelayedGap(stocks);
    HashMap<String, Data> dataMap = readHistoricalDataFromActiveTick("data/quotes");
    Result result = buyOnGap.runWith(dataMap, 30000, new Params(6, 59),
        to.minusYears(1), to);
    System.out.println("size of valSeries: " + result.valSeries.size());
    Canvas canvas = new Canvas("output/perf.html");
    canvas.addSeries(stocks.getStockFromDB("SPY").alignWith(result.valSeries)
        .normalize().setName("SPY"));
    canvas.addSeries(result.valSeries.normalize());
    canvas.draw();
  }

  private static HashMap<String, Data> readHistoricalDataFromActiveTick(
      String string) throws IOException {
    System.out.println("start reading historical data...");
    HashMap<String, Data> map = new HashMap<String, Data>();
    BufferedReader br = new BufferedReader(new FileReader(string));
    String line;
    while ((line = br.readLine()) != null) {
      // e.g. : A 2015-06-22T09:30 39.94 3 39.75 5
      String[] tokens = line.split("\\s+");
      String sym = tokens[0];
      String time = tokens[1];
      String ask = tokens[2];
      String bid = tokens[4];
      String tokenComb = tokens[0] + " " + time;
      if (time.endsWith("09:30") || time.endsWith("15:59") || 
          time.endsWith("09:30:58") || time.endsWith("09:30:59")) {
      map.putIfAbsent(tokenComb,
          new Data(sym, time).setAsk(Double.valueOf(ask))
              .setBid(Double.valueOf(bid))); // ask price
      }
    }
    br.close();
    System.out.println("reading historical data done. map size " + map.size());
    return map;
  }
}
