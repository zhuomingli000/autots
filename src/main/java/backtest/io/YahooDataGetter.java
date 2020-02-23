package backtest.io;

import backtest.struct.Table;
import backtest.utils.LabeledLatch;
import backtest.utils.Util;
import com.google.common.util.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.*;
import java.util.function.Consumer;

public class YahooDataGetter {
  private static Logger logger = LoggerFactory.getLogger(YahooDataGetter.class);
  private final LabeledLatch latch;
  private final ListeningExecutorService executor;
  private boolean stopped;

  public YahooDataGetter(int numProducers) {
    executor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(numProducers));
    latch = new LabeledLatch();
    stopped = false;
  }

  public void download(String ticker, Consumer<Table> consumer) {
    if (stopped) {
      logger.error("stopped");
      return;
    }
    latch.register(ticker);
    ListenableFuture<Table> table = executor.submit(()->{
      Util.printAtSameLine("downloading " + ticker);
      String url = "http://ichart.finance.yahoo.com/table.csv?s=" + ticker;
      return new CSVReader().readTableFromUrl(url);
    });
    Futures.addCallback(table, new FutureCallback<Table>() {
      @Override
      public void onSuccess(Table table) {
        latch.arrive(ticker);
        table.setName(ticker);
        consumer.accept(table);
      }

      @Override
      public void onFailure(Throwable t) {
        logger.error("Error", t);
        latch.arrive(ticker);
      }
    });
  }

  public void await() {
    stopped = true;
    executor.shutdown();
    latch.await(Duration.ofMinutes(20));
  }
}
