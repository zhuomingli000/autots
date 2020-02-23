package backtest.utils;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.ArrayBlockingQueue;

public class RateLimiter {
  private final ArrayBlockingQueue<DelayedEvent> q;
  private final Duration d;

  public RateLimiter(int n, Duration d) {
    this.q = new ArrayBlockingQueue<>(n, true);
    this.d = d;
    for (int i = 0; i < n; i++) q.add(new DelayedEvent(Duration.ofNanos(0)));
  }

  private class DelayedEvent {
    private final LocalDateTime startedAt;
    private final Duration delay;

    public DelayedEvent(Duration dd) {
      this.startedAt = LocalDateTime.now();
      this.delay = dd;
    }

    public void delay() {
      long toWaitMillis = Duration.between(LocalDateTime.now(), startedAt.plus(this.delay)).toMillis();
      if (toWaitMillis > 0) {
        try {
          Thread.sleep(toWaitMillis);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
  }

  public void acquire() {
    try {
      q.take().delay();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public void release() {
    try {
      q.put(new DelayedEvent(d));
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) {
    RateLimiter r = new RateLimiter(2, Duration.ofMillis(500));
    for (int i = 0; i < 8; i++) {
      System.out.println(i);
      System.out.println(LocalDateTime.now() + ". Will wait.");
      r.acquire();
      System.out.println(LocalDateTime.now() + ". Start exec.");
      r.release();
      System.out.println(LocalDateTime.now());
    }
  }
}
