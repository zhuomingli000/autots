package backtest.struct;

import backtest.utils.Num;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.BinaryOperator;

/**
 * keySet of TimeSeries is in strictly increasing order.
 */
public class TimeSeries extends NumMap<LocalDate> {
  private final static Logger logger = LoggerFactory.getLogger(TimeSeries.class);
  private final static BinaryOperator<Double> division = (x, y) -> x / y;
  private final static BinaryOperator<Double> subtraction = (x, y) -> x - y;
  private final static BinaryOperator<Double> addition = (x, y) -> x + y;
  private String name;
  private final String keyName = "keys";

  public TimeSeries() {
    this("Series");
  }

  public TimeSeries(String name) {
    super();
    this.name = name;
  }

  public TimeSeries(TimeSeries series) {
    super(series);
  }

  public TimeSeries(Map<LocalDate, Double> map) {
    this(new TreeMap<>(map));
  }

  public TimeSeries(TreeMap<LocalDate, Double> map) {
    this();
    for (Map.Entry<LocalDate, Double> entry : map.entrySet()) {
      put(entry.getKey(), entry.getValue());
    }
  }

  public TimeSeries(LocalDate from, LocalDate to, double initValue) {
    this();
    for (LocalDate day = from; day.isBefore(to); day = day.plusDays(1)) {
      put(day, initValue);
    }
  }

  @Override
  public TimeSeries put(LocalDate date, double val) {
    if (size() == 0 || date.isAfter(lastKey())) super.put(date, val);
    return this;
  }

  public double sum() {
    double sum = 0;
    for (Map.Entry<LocalDate, Num> entry : entrySet()) {
      sum += entry.getValue().toDouble();
    }
    return sum;
  }

  public double mean() {
    if (size() == 0) return Double.NaN;
    double sum = 0;
    for (Map.Entry<LocalDate, Num> entry : entrySet()) {
      sum += entry.getValue().toDouble();
    }
    return sum / (double) size();
  }

  //var = E[(X-mu)^2] = E[X^2]-E[X]^2
  public double std() {
    if (size() == 0) return Double.NaN;
    double squareSum = 0;
    for (Map.Entry<LocalDate, Num> entry : entrySet()) {
      squareSum += (entry.getValue().toDouble() * entry.getValue().toDouble());
    }
    double mean = mean();
    return Math.sqrt(squareSum / (double) size() - mean * mean);
  }

  public TimeSeries divide(double v) {
    TimeSeries ret = new TimeSeries();
    for (Map.Entry<LocalDate, Num> entry : entrySet()) {
      ret.put(entry.getKey(), entry.getValue().toDouble() / v);
    }
    return ret;
  }

  public TimeSeries divide(TimeSeries series) {
    return strictOp(series, division);
  }

  public TimeSeries subtract(TimeSeries series) {
    return looseOp(series, subtraction);
  }

  public TimeSeries add(TimeSeries series) {
    return looseOp(series, addition);
  }

  public TimeSeries strictOp(TimeSeries series, BinaryOperator<Double> operator) {
    Iterator<Map.Entry<LocalDate, Num>> thisIt = iterator(), thatIt = series.iterator();
    TimeSeries ret = new TimeSeries();
    Map.Entry<LocalDate, Num> thisEntry = thisIt.hasNext() ? thisIt.next() : null,
        thatEntry = thatIt.hasNext() ? thatIt.next() : null;
    while (thisEntry != null && thatEntry != null) {
      if (thisEntry.getKey().isBefore(thatEntry.getKey())) {
        thisEntry = thisIt.hasNext() ? thisIt.next() : null;
      } else if (thisEntry.getKey().isEqual(thatEntry.getKey())) {
        ret.put(thisEntry.getKey(), operator.apply(thisEntry.getValue().toDouble(), thatEntry.getValue().toDouble()));
        thisEntry = thisIt.hasNext() ? thisIt.next() : null;
        thatEntry = thatIt.hasNext() ? thatIt.next() : null;
      } else {
        thatEntry = thatIt.hasNext() ? thatIt.next() : null;
      }
    }
    return ret;
  }

  public TimeSeries looseOp(TimeSeries series, BinaryOperator<Double> operator) {
    Iterator<Map.Entry<LocalDate, Num>> thisIt = iterator(), thatIt = series.iterator();
    TimeSeries ret = new TimeSeries();
    Map.Entry<LocalDate, Num> thisEntry = thisIt.hasNext() ? thisIt.next() : null,
        thatEntry = thatIt.hasNext() ? thatIt.next() : null;
    while (thisEntry != null && thatEntry != null) {
      if (thisEntry.getKey().isBefore(thatEntry.getKey())) {
        ret.put(thisEntry.getKey(), operator.apply(thisEntry.getValue().toDouble(), 0d));
        thisEntry = thisIt.hasNext() ? thisIt.next() : null;
      } else if (thisEntry.getKey().isEqual(thatEntry.getKey())) {
        ret.put(thisEntry.getKey(), operator.apply(thisEntry.getValue().toDouble(), thatEntry.getValue().toDouble()));
        thisEntry = thisIt.hasNext() ? thisIt.next() : null;
        thatEntry = thatIt.hasNext() ? thatIt.next() : null;
      } else {
        ret.put(thatEntry.getKey(), operator.apply(0d, thatEntry.getValue().toDouble()));
        thatEntry = thatIt.hasNext() ? thatIt.next() : null;
      }
    }
    if (thisEntry != null) {
      ret.put(thisEntry.getKey(), operator.apply(thisEntry.getValue().toDouble(), 0d));
      while (thisIt.hasNext()) {
        thisEntry = thisIt.next();
        ret.put(thisEntry.getKey(), operator.apply(thisEntry.getValue().toDouble(), 0d));
      }
    } else if (thatEntry != null) {
      ret.put(thatEntry.getKey(), operator.apply(0d, thatEntry.getValue().toDouble()));
      while (thatIt.hasNext()) {
        thatEntry = thatIt.next();
        ret.put(thatEntry.getKey(), operator.apply(0d, thatEntry.getValue().toDouble()));
      }
    }
    return ret;
  }

  public TimeSeries normalize() {
    return divide(firstValue().toDouble());
  }

  // Probably better to present in a key value pair format.
  // Currently: { keys: [a,b,c], values: [1,2,3], name: a }
  // Ideal: { name: a, data: {a:1, b:2, c:3} }
  public String toString() {
    boolean first = true;
    StringBuilder sb = new StringBuilder("{" + keyName + ":[");
    for (Map.Entry<LocalDate, Num> entry : entrySet()) {
      if (!first) sb.append(',');
      sb.append('\'');
      sb.append(entry.getKey());
      sb.append('\'');
      first = false;
    }
    sb.append("],values:[");
    first = true;
    for (Map.Entry<LocalDate, Num> entry : entrySet()) {
      if (!first) sb.append(',');
      sb.append(entry.getValue().toDouble());
      first = false;
    }
    sb.append("],name:'");
    sb.append(name);
    sb.append("'}");
    return sb.toString();
  }

  public TimeSeries setName(String name) {
    this.name = name;
    return this;
  }

  public double get(LocalDate date) {
    Map.Entry<LocalDate, Num> e = getEntry(date);
    if (e == null) return Double.NaN;
    return e.getValue().toDouble();
  }

  public double getOrDefault(LocalDate date) {
    double v = get(date);
    if (Double.isNaN(v)) return 0;
    return v;
  }

  public Map.Entry<LocalDate, Num> getEntry(LocalDate date) {
    int from = 0;
    int to = size() - 1;
    while (from <= to) {
      int mid = from + (to - from) / 2;
      Map.Entry<LocalDate, Num> e = getEntry(mid);
      if (e.getKey().isEqual(date)) return e;
      else if (e.getKey().isBefore(date)) from = mid + 1;
      else to = mid - 1;
    }
    return null;
  }

  public TimeSeries getRet(int retPeriod) {
    TimeSeries ret = new TimeSeries();
    for (int i = 0; i<size(); i++) {
      if (i < retPeriod) continue;
      double from = getValue(i-retPeriod).toDouble();
      double to = getValue(i).toDouble();
      ret.put(getKey(i), (to-from)/from);
    }
    return ret;
  }
}
