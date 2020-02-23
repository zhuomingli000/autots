package backtest.utils;

/**
 * Double
 */
public class Num implements Comparable<Num> {
  private double eps;
  private double v;

  public Num() {
    v = 0;
    eps = 1e-5;
  }

  public Num(double v) {
    this.v = v;
    this.eps = 1e-5;
  }

  public Num(double v, double eps) {
    this.v = v;
    this.eps = eps;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null) return false;
    if (o instanceof Num) {
      Num n = (Num) o;
      if (Math.abs(v - n.v) < eps) return true;
    }
    return false;
  }

  @Override
  public int compareTo(Num o) {
    if (equals(o)) return 0;
    return v > o.v ? 1 : -1;
  }

  public double toDouble() {
    return v;
  }

  public String toString() {
    return Double.toString(toDouble());
  }

  public Num add(Num n) {
    return new Num(v + n.toDouble());
  }
}