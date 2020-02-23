package backtest.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class LabeledSemaphore extends LabeledLatch {
  private static final Logger logger = LoggerFactory.getLogger(LabeledSemaphore.class);

  private final Semaphore semaphore;

  public LabeledSemaphore(int n) {
    super();
    semaphore = new Semaphore(n);
  }

  // Makes more sense to tryAcquire instead of acquire. But TimedSemaphore is preferred over
  // LabeledSemaphore. In TimedSemaphore, permits always get released after timeout. so acquire
  // never waits forever here.
  @Override
  public boolean register(String label) {
    try {
      semaphore.acquire();
    } catch (InterruptedException e) {
      logger.error("semaphore.acquire interrupted while registering {}", label);
      Thread.currentThread().interrupt();
      return false;
    }
//    System.out.println("register " + label);
    return super.register(label);
  }

  @Override
  public boolean arrive(String label) {
    if (super.arrive(label)) {
      semaphore.release();
//      System.out.println("arrive " + label);
      return true;
    } else {
      return false;
    }
  }

  public int availablePerimits() {
    return semaphore.availablePermits();
  }
}