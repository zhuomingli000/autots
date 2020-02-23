package backtest.utils;

import java.time.Duration;

public class LabeledRateLimiter extends LabeledLatch {
  private final RateLimiter rateLimiter;

  public LabeledRateLimiter(int n, Duration d) {
    super();
    rateLimiter = new RateLimiter(n, d);
  }
  @Override
  public boolean register(String label) {
    rateLimiter.acquire();
    return super.register(label);
  }

  @Override
  public boolean arrive(String label) {
    if (super.arrive(label)) {
      rateLimiter.release();
      System.out.println("arrive " + label);
      return true;
    } else {
      System.out.println("arrive called for " + label + ". But label has already been removed.");
      return false;
    }
  }
}