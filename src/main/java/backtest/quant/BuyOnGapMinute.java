package backtest.quant;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtest.struct.Data;
import backtest.struct.TimeSeries;
import backtest.utils.Cal;
import backtest.utils.StopWatch;

import com.google.common.collect.Table;
import com.google.common.collect.TreeBasedTable;

public class BuyOnGapMinute {
  public class Params {
    int bp, n;
    double beart, bt, bub, sw;

    Params(int bp, double beart, double bt, double bub, double sw, int n) {
      this.beart = beart;
      this.bt = bt;
      this.bub = bub;
      this.sw = sw;
      this.n = n;
    }

    public String toString() {
      return "bear period " + bp + "\nbear thresh " + beart + "\nbest thresh "
          + bt + "\nbest upper bound " + bub + "\nstop win: " + sw
          + " max # of orders: " + n;
    }
  }

  enum Strategy {
    PARTIAL_GAP_DOWN, PARTIAL_GAP_UP, PARTIAL_GAP_DOWN_ADJUSTED, FULL_GAP_DOWN, FULL_GAP_DOWN_ADJUSTED;
  }

  public void test(Strategy strategy) throws IOException {
    HashMap<String, Data> dataMap = readHistoricalData("data/2014hh");

    LocalDate fromDate = Cal
        .getLatestBusinessDayAfter(LocalDate.of(2014, 1, 1));
    LocalDate toDate = Cal.getLatestBusinessDayBefore(LocalDate.of(2015, 1, 1));
    List<String> sp500 = new Stocks().getSP500Online();
    double bestTotal = 0;
    double bestThresh = 0;
    double bestUb = 0;
    int bestNumTrade = 0;
    int bestNumHitUp = 0;

    // iterate all cases.
    for (double thresh = 0.05; thresh > 0.02; thresh -= 0.005) {
      StopWatch watch = new StopWatch();
      for (double ub = 0.5; ub < 1.5; ub += 0.2) {
        System.out.println("thresh: " + thresh);
        System.out.println("ub: " + ub);
        double total = 30000;
        int numT = 0;
        int numHitUp = 0;
        int numRetrieved = 0;
        // iterate all days
        for (LocalDate date = fromDate; date.isBefore(toDate); date = Cal
            .getNextBusinessDay(date)) {
          for (LocalTime time = LocalTime.of(6, 30); time.isBefore(LocalTime
              .of(12, 00)); time = time.plusMinutes(30)) {
            Map<String, GapTicker> boughtTickers = new HashMap<>();
            for (String ticker : sp500) {
              // calc gap
              Data data = null;
              double gap = 0;
              if (Strategy.PARTIAL_GAP_DOWN_ADJUSTED == strategy) {
                // gap = stock.getGap(i) - spy.getGap(spyi);
              } else if (Strategy.PARTIAL_GAP_DOWN == strategy) {
                String key = ticker + " " + date.toString() + " "
                    + time.format(DateTimeFormatter.ISO_LOCAL_TIME);
                data = dataMap.get(key);
                if (data == null) {
                  continue;
                }
                gap = (data.getOpen() - data.getClose()) / data.getOpen();
              } else if (Strategy.FULL_GAP_DOWN == strategy) {
                // gap = stock.getFullGap(i);
              } else if (Strategy.FULL_GAP_DOWN_ADJUSTED == strategy) {
                // gap = stock.getFullGap(i) - spy.getFullGap(spyi);
              } else {
                throw new IllegalArgumentException("strategy not implementd");
              }
              if (Double.isNaN(gap) || gap < thresh)
                continue;
              if (total - 150 * data.getClose() < 0)
                continue;
              boughtTickers.put(ticker,
                  new GapTicker(ticker, gap, data.getClose(), 0));
              total -= 2;
              total -= 150 * data.getClose();
              numT++;
            }
            for (String ticker : boughtTickers.keySet()) {
              Data next30Min = dataMap.get(ticker
                  + " "
                  + date.toString()
                  + " "
                  + time.plusMinutes(30).format(
                      DateTimeFormatter.ISO_LOCAL_TIME));
              if (next30Min == null) {
                total += 150 * boughtTickers.get(ticker).startPrice;
                numT--;
              } else {
                if ((next30Min.getHigh() - next30Min.getOpen())
                    / next30Min.getOpen() >= ub * thresh) {
                  total += 150 * (next30Min.getOpen() * (1+ub * thresh));
                  numHitUp++;
                } else {
                total += 150 * next30Min.getClose();
                }
              }
            }
            boughtTickers = new HashMap<>();
          }
        }
        System.out.println("total : " + total);
        System.out.println("num retrieved " + numRetrieved);
        if (total >= bestTotal) {
          bestTotal = total;
          bestThresh = thresh;
          bestUb = ub;
          bestNumHitUp = numHitUp;
          bestNumTrade = numT;
        }
      }
    }
    if (bestThresh != 0) {
      System.out.println("best total: " + bestTotal);
      System.out.println("best thresh: " + bestThresh);
      System.out.println("best ub: " + bestUb);
      System.out.println("best num hit up avg: " + bestNumHitUp/250);
      System.out.println("best num trade avg: "+ bestNumTrade/250);
    } else {
      System.out.println("no result.");
    }
  }

  private HashMap<String, Data> readHistoricalData(String string)
      throws IOException {
    HashMap<String, Data> map = new HashMap<String, Data>();
    BufferedReader br = new BufferedReader(new FileReader(string));
    String line;
    while ((line = br.readLine()) != null) {
      String[] tokens = line.split("\\s+");
      String tokenComb = tokens[0] + " " + tokens[1] + " " + tokens[2];
      map.put(tokenComb, new Data(tokens[0], tokens[1] + " " + tokens[2],
          tokens[3], tokens[4], tokens[5], tokens[6], tokens[7]));
    }
    return map;
  }

  private static boolean isValidResult(String[] tokens) {
    if (tokens.length < 8)
      return false;
    return true;
  }

  public static void main(String[] args) throws IOException {
    new BuyOnGapMinute().test(Strategy.PARTIAL_GAP_DOWN);
  }
}
