package backtest.struct;

import backtest.utils.Num;

import java.time.LocalDate;
import java.util.Comparator;
import java.util.Map;

public class NumMap<K extends Comparable<? super K>> extends ArrayMap<K, Num> {
  public NumMap() {
    super();
  }

  public NumMap(NumMap<K> series) {
    super(series);
  }

  public ArrayMap<K, Num> put(K key, double val) {
    if (Double.isNaN(val)) return this;
    return super.put(key, new Num(val));
  }

  public double max() {
    return extremeValue(Comparator.naturalOrder());
  }

  public double min() {
    return extremeValue(Comparator.reverseOrder());
  }

  private double extremeValue(Comparator<Double> cmp) {
    double ret = firstValue().toDouble();
    for (Map.Entry<K, Num> e : entrySet()) {
      double v = e.getValue().toDouble();
      if (cmp.compare(v, ret) > 0) ret = v;
    }
    return ret;
  }

  public class Drawdown {
    public double drawdown;
    public K from, to;
    public Drawdown(double drawdown, K from, K to) {
      this.drawdown = drawdown;
      this.from = from;
      this.to = to;
    }
    public String toString(){
      return "max drawdown: " + drawdown + " from " + from + " to " + to;
    }
  }

  public Drawdown maxDrawdown(){
    K maxKey = firstKey();
    double max = firstValue().toDouble();
    double drawDown = 0;
    K ddFrom = null, ddTo = null;
    for (Map.Entry<K, Num> entry : entrySet()) {
      double val = entry.getValue().toDouble();
      if (val > max) {
        max = val;
        maxKey = entry.getKey();
      }
      if ((max - val) / max > drawDown) {
        drawDown = (max - val) / max;
        ddFrom = maxKey;
        ddTo = entry.getKey();
      }
    }
    if (Math.abs(drawDown) < 1e-8 || ddFrom == null || ddTo == null) {
      return null;
    } else {
      return new Drawdown(drawDown, ddFrom, ddTo);
    }
  }
}
