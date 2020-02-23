package backtest.ib;

import backtest.utils.RateLimiter;
import com.ib.client.TickType;
import com.ib.client.Types;
import com.ib.controller.ApiController;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class TopMktData {
  private final Set<String> subscribed;
  private final IBController ibController;
  private final RateLimiter rateLimiter;
  private ConcurrentMap<String, TopMktDataConsumer<String, TickType, Double>> consumers;

  public TopMktData(IBController ibController){
    this.subscribed = new HashSet<>();
    this.ibController = ibController;
    this.rateLimiter = new RateLimiter(80, Duration.ofSeconds(1));
    this.consumers = new ConcurrentHashMap<>();
  }

  private class Handler implements ApiController.ITopMktDataHandler {
    private final String ticker;

    // IB could pass ticker into tickPrice() but they are not. For now, pass ticker by myself.
    public Handler(String ticker) {
      this.ticker = ticker;
    }

    @Override
    public void tickPrice(TickType tickType, double price, int canAutoExecute) {
      for (TopMktDataConsumer<String, TickType, Double> consumer : consumers.values()) {
        consumer.process(ticker, tickType, price);
      }
    }

    @Override
    public void tickSize(TickType tickType, int size) {}

    @Override
    public void tickString(TickType tickType, String value) {}

    @Override
    public void tickSnapshotEnd() {}

    @Override
    public void marketDataType(Types.MktDataType marketDataType) {}
  }

  public synchronized void subscribe(String ticker) {
    if (!subscribed.contains(ticker)) {
      rateLimiter.acquire();
      ibController.subscribeTopMktData(ticker, new Handler(ticker));
      rateLimiter.release();
      subscribed.add(ticker);
    }
  }

  public void addListener(String consumerId, TopMktDataConsumer<String, TickType, Double> consumer) {
    consumers.put(consumerId, consumer);
  }

  public void removeListener(String consumerId) {
    consumers.remove(consumerId);
  }
}
