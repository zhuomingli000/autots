package backtest.utils;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class TimedSemaphore<LABEL> {
  private static final Logger logger = LoggerFactory.getLogger(TimedSemaphore.class);
  private final Multiset<LABEL> set;
  private final ReentrantLock lock;
  private final Condition done;
  private boolean stopped, completed;
  private final Semaphore semaphore;
  private final DelayQueue<Timeout> delayQueue;
  private final ConcurrentMap<LABEL, Boolean> timeoutMap;
  private final Duration timeout;

  public TimedSemaphore(int n, Duration timeout) {
    set = HashMultiset.create();
    lock = new ReentrantLock();
    done = lock.newCondition();
    stopped = false;
    completed = false;
    semaphore = new Semaphore(n);
    delayQueue = new DelayQueue<>();
    timeoutMap = new ConcurrentHashMap<>();
    this.timeout = timeout;
    Thread thread = new Thread(new TimeoutThread());
    thread.setName("TimedSemaphore: time out thread");
    thread.setDaemon(true);
    thread.start();
  }

  private class Timeout implements Delayed {
    final LABEL label;
    final LocalDateTime delayTo;

    Timeout(LABEL label) {
      this(label, LocalDateTime.now().plus(timeout));
    }

    private Timeout(LABEL label, LocalDateTime delayTo) {
      this.label = label;
      this.delayTo = delayTo;
//      System.out.println("new timeout ends at " + delayTo);
    }

    @Override
    public long getDelay(TimeUnit unit) {
//      System.out.println(unit.convert(Duration.between(LocalDateTime.now(), delayTo).toMillis(), TimeUnit.MILLISECONDS));
      return unit.convert(Duration.between(LocalDateTime.now(), delayTo).toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed o) {
      long diff = getDelay(TimeUnit.MILLISECONDS) - o.getDelay(TimeUnit.MILLISECONDS);
      if (diff < 0) return -1;
      else if (diff == 0) return 0;
      else return 1;
    }

    LABEL getLabel() {return label;}

    boolean isEnd() {return false;}
  }

  private final class End extends Timeout {
    private End() {
      super(null, LocalDateTime.now());
    }
    @Override
    boolean isEnd() {return true;}
  }

  private class TimeoutThread implements Runnable {
    @Override
    public void run() {
      while (true) {
        try {
          Timeout timeout = delayQueue.take();
          LABEL label = timeout.getLabel();
          if (timeout.isEnd()) {
//            System.out.println("timed semaphore ends");
            break;
          }
          if (arrive(label)) {
            logger.info("{} is timed out", label);
            timeoutMap.put(label, true);
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }
  }

  // Returns false if label is not registered.
  public boolean register(LABEL label) {
    boolean ret = false;
    lock.lock();
    try {
      if (stopped) {
        logger.error("TimedSemaphore has been stopped.");
      } else {
        set.add(label);
        delayQueue.add(new Timeout(label));
        ret = true;
      }
    } finally {
      lock.unlock();
    }
    try {
      if (ret) {
        // Semaphore has to be acquired and released out of locking. Otherwise deadlock could
        // happen if main thread is waiting on registering, while TimeoutThread is trying to
        // release one permit from semaphore.
        semaphore.acquire();
      }
    } catch (InterruptedException e) {
      logger.error("semaphore.acquire interrupted while registering {}", label);
      Thread.currentThread().interrupt();
      return false;
    }
    return ret;
  }

  // Returns true if arrival removed label from internal set.
  public boolean arrive(LABEL label) {
    lock.lock();
    try {
      if (completed) {
        return false;
      }
      boolean ret = set.remove(label);
      timeoutMap.remove(label);
      if (ret) {
        semaphore.release();
      }
      if (set.isEmpty()) {
        done.signalAll();
        delayQueue.add(new End());
      }
      return ret;
    } finally {
      lock.unlock();
    }
  }

  public Set<LABEL> getTimeoutSet() {return timeoutMap.keySet();}

  private void print(int n) {
    lock.lock();
    try {
      String msg = "waiting items:\n";
      int i = 0;
      for (LABEL item : set) {
        if (i>n) break;
        i++;
        msg += item + "\n";
      }
      logger.error(msg);
    } finally {
      lock.unlock();
    }
  }

  public void await() {
    lock.lock();
    try {
      stopped = true;
      while (!set.isEmpty()) {
        done.await();
      }
      completed = true;
    } catch (InterruptedException e) {
      logger.error("LabeledLatch.await is interrupted");
      print(10);
      Thread.currentThread().interrupt();
    } finally {
      lock.unlock();
    }
  }
}
