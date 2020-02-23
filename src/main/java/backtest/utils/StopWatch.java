package backtest.utils;

public class StopWatch {
  private long startTime;
  private boolean print = true;

  public void suppressOutput() {
    print = false;
  }

  public void start() {
    startTime = System.nanoTime();
  }

  public long stop() {
    long mseconds = (System.nanoTime() - startTime) / 1000000;
    long totalms = mseconds;
    long seconds = mseconds / 1000;
    mseconds = mseconds - 1000 * seconds;
    long mins = seconds / 60;
    seconds = seconds - 60 * mins;
    if (print)
      System.out.println(String.format("%d min, %d sec, %d ms", mins, seconds, mseconds));
    return totalms;
  }
}
