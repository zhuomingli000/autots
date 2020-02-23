package backtest.regime;

import backtest.quant.Stock;
import backtest.quant.Stocks;
import backtest.struct.TimeSeries;
import backtest.utils.Cal;
import backtest.utils.StopWatch;
import backtest.utils.Util;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TransThreshFinder {
  private final static StopWatch sw = new StopWatch();

  public static void main(String[] args) {
    Stocks stocks = new Stocks();
    stocks.cacheAllStockQueries();
    stocks.updateSP500AndSPYTo(LocalDate.of(2016, 2, 1));
    // TODO: possibly strip off 09 crisis.
    LocalDate from = LocalDate.of(2009, 4, 1);
    LocalDate to = LocalDate.of(2016, 1, 22);
    TransProbFinder transProbFinder = new TransProbFinder(from, to);
    boolean first = true;
    double bearProb = 0.01;
    double bullProb = 1 - bearProb;
    double bestfmeasure = -1;

    TimeSeries bestBearProb = null;
    TimeSeries bestTrans = null;
    TimeSeries bestEmit = null;
    List<LocalDate> bestBearList = null;
    int bestWinSize = -1;
    double bestThresh = -1;
    double bearToBear = transProbFinder.pBearToBear;
    double bullToBear = transProbFinder.pBullToBear;
    double bestPrecision = -1, bestRecall = -1;
    double thresh = 0.08;
    int period = 27;
    Stock spy = stocks.getStockFromDB("SPY");
    List<LocalDate> realBearList = spy.getBearDates(from, to, thresh, period);
    Set<LocalDate> realBearSet = new HashSet<>(realBearList);
    for (int winSize = 4; winSize < 36; winSize++) {
      TimeSeries bearSeries = new TimeSeries("bear series");
      TimeSeries trans = new TimeSeries("trans");
      TimeSeries emit = new TimeSeries("emit");
      System.out.println("extreme win size: " + winSize);
      sw.start();
      RegimeModel model = new RegimeModel(stocks);
      model.calcHighMinusLowRate(winSize, from, to);
      sw.stop();
      double rmax = model.highMinusLow.max();
      double rmin = model.highMinusLow.min();
      // p(x|bear) = a*x + b
      // a*max+b=0
      // p(x|bear) = a*x - a*max
      // a = -1/(0.5*max^2+0.5*min^2-max*min)
      // p(x|bull) = c*x + d
      // c*min+d=0
      // p(x|bull) = c*x - c*min
      // c = -a
      // p(x|bull) = -a*x + a*min
      double a = -1 / (0.5 * rmax * rmax + 0.5 * rmin * rmin - rmax * rmin);
      if (rmax <= 0 || rmin >= 0) {
        System.out.println("invalid min or max. win size: " + winSize);
        continue;
      }
      for (double threshold = 0.1; threshold < 1; threshold += 0.1) {
        List<LocalDate> bearList = new ArrayList<>();
        for (LocalDate date = Cal.getLatestBusinessDayAfter(from); date.isBefore(to);
             date = Cal.getNextBusinessDay(date)) {
          if (!first) {
            double x = model.highMinusLow.getOrDefault(date);
            double bearTrans = (bearProb * bearToBear + (1 - bearProb) * bullToBear);
            double bullTrans = (bullProb * (1 - bullToBear) + (1 - bullProb) * (1 - bearToBear));
            double bearEmit = (a * x - a * rmax);
            double bullEmit = (-a * x + a * rmin);
            trans.put(date, bearTrans);
            emit.put(date, bearEmit);
            bearProb = bearTrans * bearEmit;
            bullProb = bullTrans * bullEmit;
            bearProb = bearProb / (bearProb + bullProb);
            bullProb = 1 - bearProb;
            bearSeries.put(date, bearProb);
            //System.out.println(bearProb);
          }

          if (bearProb > threshold) bearList.add(date);
          first = false;
        }
        double count = 0;
        for (LocalDate date : bearList) {
          if (realBearSet.contains(date)) count++;
        }
        double precision = count / bearList.size();
        double recall = count / realBearSet.size();
        double fmeasure = 2 * precision * recall / (precision + recall);
        if (fmeasure > bestfmeasure) {
          bestfmeasure = fmeasure;
          bestRecall = recall;
          bestPrecision = precision;
          bestWinSize = winSize;
          bestBearProb = bearSeries;
          bestEmit = emit;
          bestThresh = threshold;
          bestBearList = bearList;
        }
      }
    }
    if (bestfmeasure > 0) {
      System.out.println("win size: " + bestWinSize);
      System.out.println("fmeasure: " + bestfmeasure);
      System.out.println("recall: " + bestRecall);
      System.out.println("precision: " + bestPrecision);
      System.out.println("threshold: " + bestThresh);
      System.out.println("real bear list");
      Util.printColl(realBearList);
      System.out.println("get bear list");
      Util.printColl(bestBearList);
//            Canvas canvas = new Canvas("output/perf.html");
//
//            canvas.addSeries(stocks.getStockFromCache("SPY").alignWith(cumRet).normalize().setName("SPY"));
//
//            canvas.addSeries(bestBearProb, 1);
//            canvas.addSeries(bestEmit, 2);
//            canvas.draw();
    }
  }
}
