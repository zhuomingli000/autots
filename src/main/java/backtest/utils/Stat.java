package backtest.utils;

import java.util.Set;

public class Stat {
  public static double mean(double[] a) {
    if (a.length == 0) return Double.NaN;
    double sum = 0;
    for (double num : a) sum += num;
    return sum / a.length;
  }

  public static double var(double[] a) {
    if (a.length == 0) return Double.NaN;
    double avg = mean(a);
    double sum = 0;
    for (double num : a) sum += (num - avg) * (num - avg);
    return sum / a.length;
  }

  public static double std(double[] a) {
    if (a.length == 0) return Double.NaN;
    return Math.sqrt(var(a));
  }

  public static <T> double fmeasure(Set<T> real, Set<T> get) {
    double count = 0;
    for (T t : get) {
      if (real.contains(t)) count++;
    }
    double precision = count / get.size();
    double recall = count / real.size();
    return 2 * precision * recall / (precision + recall);
  }

  public static <T> double precision(Set<T> real, Set<T> get) {
    double count = 0;
    for (T t : get) {
      if (real.contains(t)) count++;
    }
    return count / get.size();
  }

  public static <T> double recall(Set<T> real, Set<T> get) {
    double count = 0;
    for (T t : get) {
      if (real.contains(t)) count++;
    }

    return count / real.size();
  }
}
