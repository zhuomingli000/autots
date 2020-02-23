package backtest.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.*;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Scheduler {
  private final static Logger logger = LoggerFactory.getLogger(Scheduler.class);
  private final static String timeZone = "America/New_York";
  private ZoneId zone;

  public Scheduler() {
    zone = ZoneId.of(timeZone);
  }

  public void schedule(final Task task, int hour, int min) {
    final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
    executeAt(task, executorService, hour, min);
    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        logger.info("shutting down " + task + "...");
        executorService.shutdownNow();
        try {
          if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
            logger.info(task + " did not terminate in the specified time.");
            List<Runnable> droppedTasks = executorService.shutdownNow();
            logger.info(task + " was abruptly shut down. " + droppedTasks.size() + " tasks will not be executed.");
          }
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    });
  }

  private void executeAt(final Task task, final ScheduledExecutorService executorService, final int targetHour,
                         final int targetMin) {
    Runnable taskWrapper = () -> {
      LocalDateTime now = LocalDateTime.now();
      task.perform(ZonedDateTime.of(now, zone).toLocalDate());
      executeAt(task, executorService, targetHour, targetMin);
    };
    Duration delay = computeNextDelay(targetHour, targetMin);
    logger.info("scheduled " + task + " at " + targetHour + ":" + targetMin);
    logger.info("will sleep: " + delay);
    executorService.schedule(taskWrapper, delay.getSeconds(), TimeUnit.SECONDS);
  }

  private Duration computeNextDelay(int targetHour, int targetMin) {
    ZoneId easternTime = ZoneId.of("America/New_York");
    ZonedDateTime now = ZonedDateTime.now().withZoneSameInstant(easternTime);
    ZonedDateTime to = now.withHour(targetHour).withMinute(targetMin).withSecond(0);
    while (!now.isBefore(to)) {
      to = to.with(Cal.getNextBusinessDay(to.toLocalDate()));
    }
    return Duration.between(now, to);
  }
}
