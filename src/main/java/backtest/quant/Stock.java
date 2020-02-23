package backtest.quant;

import backtest.struct.TimeSeries;
import backtest.utils.Cal;
import backtest.utils.Stat;
import backtest.utils.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.Period;
import java.util.*;

public class Stock {
  private final static Logger logger = LoggerFactory.getLogger(Stock.class);
  private ArrayList<LocalDate> dates;
  private ArrayList<Double> adjClose;
  private ArrayList<Double> high;
  private ArrayList<Double> low;
  private ArrayList<Double> open;
  private ArrayList<Double> close;
  private String ticker;

  public Stock() {
    dates = new ArrayList<>();
    adjClose = new ArrayList<>();
    high = new ArrayList<>();
    low = new ArrayList<>();
    open = new ArrayList<>();
    close = new ArrayList<>();
  }

  public Stock(String ticker, Connection con) {
    this();
    this.ticker = ticker;
    load(con);
  }

  private void load(Connection con) {
    String statement = "SELECT dp.* FROM daily_price AS dp INNER JOIN symbol AS sym ON dp" +
        ".symbol_id=sym.id WHERE sym.ticker='" + ticker + "'";
    try (Statement st = con.createStatement();
         ResultSet rs = st.executeQuery(statement)) {
      while (rs.next()) {
        dates.add(rs.getDate("price_date").toLocalDate());
        adjClose.add(rs.getDouble("adj_close_price"));
        high.add(rs.getDouble("high_price"));
        low.add(rs.getDouble("low_price"));
        open.add(rs.getDouble("open_price"));
        close.add(rs.getDouble("close_price"));
      }
    } catch (SQLException e) {
      logger.error("error while loading stock: {}", e.getMessage());
      e.printStackTrace();
    }
  }

  public double adjCloseMovAvg(LocalDate end, int window) {
    int bound = Collections.binarySearch(dates, end);
    if (bound < 0) return Double.NaN;
    return Stat.mean(Util.coll2Arr(adjClose, bound - window < 0 ? 0 : bound - window, bound -
        1));
  }

  public double getAdjClose(LocalDate day) {
    int i = Collections.binarySearch(dates, day);
    if (i < 0) return Double.NaN;
    return adjClose.get(i);
  }

  public double getAdjClose(int i) {
    return adjClose.get(i);
  }

  public double getAdjOpen(LocalDate day) {
    int i = Collections.binarySearch(dates, day);
    if (i < 0) return Double.NaN;
    return getAdjOpen(i);
  }

  public double getAdjHigh(LocalDate day) {
    int i = Collections.binarySearch(dates, day);
    if (i < 0) return Double.NaN;
    return adjClose.get(i) / close.get(i) * high.get(i);
  }

  public LocalDate getFirstDay() {
    if (dates.isEmpty()) return null;
    return dates.get(0);
  }

  public LocalDate getLastDay() {
    if (dates.isEmpty()) return null;
    return dates.get(dates.size() - 1);
  }

  public double adjOpenRet(LocalDate day, int numDays) {
    return ret(getAdjOpen(day), getAdjOpen(Cal.getNextBusinessDay(day, numDays)));
  }

  private static double ret(double before, double after) {
    if (Double.isNaN(before) || Double.isNaN(after)) return Double.NaN;
    return after / before - 1;
  }

  public double getAdjOpen(int i) {
    return adjClose.get(i) / close.get(i) * open.get(i);
  }

  public double getAdjHigh(int i) {
    return adjClose.get(i) / close.get(i) * high.get(i);
  }

  public double getAdjLow(int i) {
    return adjClose.get(i) / close.get(i) * low.get(i);
  }

  /**
   * @param dropRate   a positive rate that indicates how much is dropped in price.
   * @param dropPeriod must be positive. unit: business days
   * @return list of bear days in order
   */
  public List<LocalDate> getBearDates(LocalDate begin, LocalDate end, double dropRate, int dropPeriod) {
    int to = Util.indexOfLastEleNoLargerThan(dates, end);
    int next = -1;
    List<LocalDate> list = new ArrayList<>();
    for (int i = Util.indexOfFirstEleNoLessThan(dates, begin); i + dropPeriod <= to; i++) {
      for (int j = 1; j <= dropPeriod; j++) {
        if (ret(adjClose.get(i), adjClose.get(i + j)) < -dropRate) {
          for (int k = Math.max(i, next); k < i + j; k++) {
            list.add(dates.get(k));
          }
          next = Math.max(i + j, next);
        }
      }
    }
    return list;
  }

  /**
   * from is adjusted internally to from = from.minus(windowSize). If from or to
   * exceeds available range of days, only available range is checked.
   * @param high if true, find days that reach window high. Otherwise, find days that
   *             reach window low
   */
  public List<LocalDate> getExtremeDays(LocalDate from, LocalDate to, Period windowSize,
                                        boolean high) {
    List<LocalDate> list = new ArrayList<>();
    if (!from.isBefore(to)) return list;
    from = from.minus(windowSize);
    int fromi = Util.indexOfFirstEleNoLessThan(dates, from);
    int toi = Util.indexOfLastEleNoLargerThan(dates, to);
    if (fromi >= toi || fromi < 0 || toi < 0) return list;

    LinkedList<LocalDate> timeQueue = new LinkedList<>();
    LinkedList<Double> valQueue = new LinkedList<>();
    for (int i = fromi; i <= toi; i++) {
      Double curPrice;
      if (high) curPrice = getAdjHigh(i);
      else curPrice = getAdjLow(i);
      LocalDate curDay = dates.get(i);
      while (!valQueue.isEmpty() && Util.distinctDoubles(curPrice, valQueue.getLast()) &&
          curPrice.compareTo(valQueue.getLast()) * (high ? 1 : -1) > 0) {
        timeQueue.removeLast();
        valQueue.removeLast();
      }
      timeQueue.add(curDay);
      valQueue.add(curPrice);
      if (curDay.minus(windowSize).isAfter(from)) {
        // Remove elements which are out of this window
        while (!timeQueue.isEmpty() && timeQueue.getFirst().isBefore(curDay.minus(windowSize))) {
          timeQueue.removeFirst();
          valQueue.removeFirst();
        }
        // The element at the front of the queue is the largest element
        // could replace below if statement by "if len==1"
        if (timeQueue.getFirst().isEqual(curDay)) {
          list.add(curDay);
//                    System.out.println(curDay);
        }
      }
    }
    return list;
  }

  public TimeSeries alignWith(TimeSeries series) {
    TimeSeries ret = new TimeSeries(ticker);
    int from = Util.indexOfFirstEleNoLessThan(dates, series.firstKey());
    int to = Util.indexOfLastEleNoLargerThan(dates, series.lastKey());
    for (int i = from; i <= to; i++) {
      ret.put(dates.get(i), adjClose.get(i));
    }
    return ret;
  }

  public TimeSeries toSeries(LocalDate begin, LocalDate end) {
    TimeSeries ret = new TimeSeries(ticker);
    int from = Util.indexOfFirstEleNoLessThan(dates, begin);
    int to = Util.indexOfLastEleNoLargerThan(dates, end);
    for (int i = from; i <= to; i++) {
      ret.put(dates.get(i), adjClose.get(i));
    }
    return ret;
  }

  public ArrayList<Double> getAdjOpenList() {
    ArrayList<Double> list = new ArrayList<>();
    for (int i = 0; i < adjClose.size(); i++) {
      list.add(adjClose.get(i) / close.get(i) * open.get(i));
    }
    return list;
  }

  public int getI(LocalDate date) {
    return Collections.binarySearch(dates, date);
  }

  public int getFirstIAfter(LocalDate date) {
    return Util.indexOfFirstEleNoLessThan(dates, date);
  }

  public int getLastIBefore(LocalDate date) {
    return Util.indexOfLastEleNoLargerThan(dates, date);
  }

  public double getGap(int i) {
    if (i < 1) return Double.NaN;
    double fromPrice = getAdjClose(i-1);
    double toPrice = getAdjOpen(i);
    return (fromPrice - toPrice) / fromPrice;
  }
  
  public double getFullGap(int i) {
    if (i < 0) return Double.NaN;
    double nowp = 0, backp = 0;
    backp = getAdjHigh(i-1);
    nowp = getAdjOpen(i);
    return (backp - nowp) / backp;
  }

  public LocalDate getDate(int i) {
    return dates.get(i);
  }

  public int findFrom(LocalDate date, int j) {
    for (int i = j; i < dates.size() && i >= 0; i++) {
      if (dates.get(i).isEqual(date)) return i;
    }
    return -1;
  }

  public double getRetTo(int i, int back) {
    if (i - back < 0) return Double.NaN;
    return ret(adjClose.get(i - back), adjClose.get(i));
  }

  public double getRet(LocalDate from, LocalDate to) {
    int fromi = getFirstIAfter(from);
    int toi = getLastIBefore(to);
    if (fromi == -1 || toi == -1 || fromi > toi) return Double.NaN;
    return ret(adjClose.get(fromi), adjClose.get(toi));
  }
}
