package backtest.ib;

import backtest.utils.RateLimiter;
import com.ib.client.TickType;
import com.ib.client.Types;
import com.ib.controller.ApiController;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;

class TopMktDataHandler implements ApiController.ITopMktDataHandler {
  private final String ticker;
  private final TickType type;
  private final RateLimiter rateLimiter;
  private final CountDownLatch latch;
  private final ConcurrentMap<String, ResultBundle> openResultBundles;

  public TopMktDataHandler(String ticker, TickType type, RateLimiter rateLimiter, CountDownLatch latch, ConcurrentMap<String, ResultBundle> openResultBundles) {
    this.ticker = ticker;
    this.type = type;
    this.rateLimiter = rateLimiter;
    this.latch = latch;
    this.openResultBundles = openResultBundles;
  }

  @Override
  public void tickPrice(TickType tickType, double price, int canAutoExecute) {
    if (tickType != type) return;
    if (price < 0) {
      System.out.println("Got negative price in tickPrice.");
      price = 100; //for testing purpose
    }
    ResultBundle r = new ResultBundle(tickType, price, canAutoExecute);
    if (type == TickType.ASK) {
      openResultBundles.put(ticker, r);
    }
  }

  @Override
  public void tickSize(TickType tickType, int size) {
    //System.out.println(String.format("tickType %s size %d", tickType.toString(), size));
  }

  @Override
  public void tickString(TickType tickType, String value) {
//        System.out.println(tickType + " " + value);
  }

  @Override
  public void tickSnapshotEnd() {
    System.out.print("\33[2K\ropenResultBundles size: " + openResultBundles.size());
    latch.countDown();
    rateLimiter.release();
  }

  @Override
  public void marketDataType(Types.MktDataType marketDataType) {
    System.out.println(marketDataType);
  }
}

