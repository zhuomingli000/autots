
package backtest.utils;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class LabeledLatch {
  private static final Logger logger = LoggerFactory.getLogger(LabeledLatch.class);
  private final Multiset<String> set;
  private final ReentrantLock lock;
  private final Condition done;
  private boolean stopped, completed;

  public LabeledLatch(){
    set = HashMultiset.create();
    lock = new ReentrantLock();
    done = lock.newCondition();
    stopped = false;
    completed = false;
  }

  public boolean register(String label) {
    lock.lock();
    try {
      if (stopped) {
        logger.error("LabeledLatch has been stopped.");
        return false;
      }
      set.add(label);
    } finally {
      lock.unlock();
    }
    return true;
  }

  public boolean arrive(String label) {
    lock.lock();
    try {
      if (completed) {
        return false;
      }
      boolean ret = set.remove(label);
//      Util.printAtSameLine(set.size() + " items are still waiting");
      if (set.isEmpty()) {
        done.signalAll();
      }
      return ret;
    } finally {
      lock.unlock();
    }
  }

  public int size() {
    lock.lock();
    try {
      return set.size();
    } finally {
      lock.unlock();
    }
  }

  private void print(int n) {
    lock.lock();
    try {
      String msg = "waiting items:\n";
      int i = 0;
      for (String item : set) {
        if (i>n) break;
        i++;
        msg += item + "\n";
      }
      logger.error(msg);
    } finally {
      lock.unlock();
    }
  }

  public void await(Duration duration) {
    lock.lock();
    try {
      stopped = true;
      LocalDateTime now = LocalDateTime.now();
      final LocalDateTime deadline = now.plus(duration);
      while (!set.isEmpty() && now.isBefore(deadline)) {
        if (!done.await(Duration.between(now, deadline).toMillis(), TimeUnit.MILLISECONDS)) {
          logger.error("time out while waiting for labeled latch. {} items are still waiting", set.size());
          print(10);
        }
        now = LocalDateTime.now();
      }
    } catch (InterruptedException e) {
      logger.error("LabeledLatch.await is interrupted");
      Thread.currentThread().interrupt();
    } finally {
      lock.unlock();
    }

  }
}
