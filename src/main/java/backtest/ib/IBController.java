package backtest.ib;

import backtest.utils.RateLimiter;
import backtest.utils.Util;
import com.ib.client.Contract;
import com.ib.client.Order;
import com.ib.client.ScannerSubscription;
import com.ib.client.TickType;
import com.ib.client.Types.BarSize;
import com.ib.client.Types.DurationUnit;
import com.ib.client.Types.WhatToShow;
import com.ib.controller.ApiController;
import com.ib.controller.ApiController.IAccountHandler;
import com.ib.controller.ApiController.IOrderHandler;
import com.ib.controller.Position;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class IBController implements ApiController.IConnectionHandler {
  private static org.slf4j.Logger logger = LoggerFactory.getLogger(IBController.class);
  private CountDownLatch connLatch;
  private boolean isConnected;
  private ApiController apiController;

  public IBController() {
    IBLogger inLogger = new IBLogger();
    IBLogger outLogger = new IBLogger();
    apiController = new ApiController(this, inLogger, outLogger);
    isConnected = false;
  }

  public void placeOrModifyOrder(Contract contract, Order order, IOrderHandler handler) {
    apiController.placeOrModifyOrder(contract, order, handler);
  }

  public boolean connect(String hostAddress, int port, int clientId) {
    connLatch = new CountDownLatch(1);
    apiController.connect(hostAddress, port, clientId, "");
    try {
      if (!connLatch.await(5, TimeUnit.SECONDS)) {
        logger.error("Failed to connect. Time out after 30 seconds");
      }
    } catch (InterruptedException e) {
      logger.error("connection request interrupted.");
      Thread.currentThread().interrupt();
    }
    return isConnected;
  }

  public void disconnect() {
    apiController.disconnect();
    logger.info("IBController disconnected");
  }

  public void reqAccountUpdates(IAccountHandler accountHandler) {
    apiController.reqAccountUpdates(true, "", accountHandler);
  }

  public void requestHistoricalData(
      String ticker, String YYYYMMDD, BarSize size, DurationUnit duration, WhatToShow whatToShow) {
    Contract contract = new Contract();
    contract.symbol(ticker);
    contract.secType("STK");
    if (ticker.equals("CSCO")
        || ticker.equals("CME")
        || ticker.equals("INTC")
        || ticker.equals("MSFT")) contract.primaryExch("NASDAQ");
    contract.exchange("SMART");
    contract.currency("USD");
    ZoneId easternTime = ZoneId.of("America/New_York");
    ZonedDateTime now = ZonedDateTime.now().withZoneSameInstant(easternTime);
    apiController.reqHistoricalData(
        contract,
        YYYYMMDD + " 20:00:00",
        //now.format(DateTimeFormatter.ofPattern("YYYYMMDD hh:mm:ss")),
        1,
        duration,
        size,
        whatToShow,
        true,
        new HistoricalDataHandler(ticker));
  }

  public void clearPositions(Map<Integer, Position> positions) {
    for (Position p : positions.values()) {
      if (p.position() > 0.01) {
        Order order = new Order();
        order.action("SELL");
        order.orderType("MKT");
        order.totalQuantity(p.position());
        OrderHandler orderHandler = new OrderHandler(p.contract(), order, "sell " + p.contract().symbol());
        apiController.placeOrModifyOrder(p.contract(), order, orderHandler);
      }
    }
  }

  public Map<String, ResultBundle> requestPrices(
      Collection<String> stocks, TickType type, Map<String, Contract> contracts)
      throws InterruptedException {
    ConcurrentMap<String, ResultBundle> openResultBundles = new ConcurrentHashMap<>();
    System.out.println("current requested stock list size: " + stocks.size());
    RateLimiter rateLimiter = new RateLimiter(90, Duration.ofSeconds(1));
    CountDownLatch topDataLatch = new CountDownLatch(stocks.size());
    for (String ticker : stocks) {
      Contract contract = new Contract();
      contract.symbol(ticker);
      contract.secType("STK");
      if (ticker.equals("CSCO")
          || ticker.equals("CME")
          || ticker.equals("INTC")
          || ticker.equals("MSFT")) contract.primaryExch("NASDAQ");
      contract.exchange("SMART");
      contract.currency("USD");
      contracts.put(ticker, contract);
      TopMktDataHandler topMktDataHandler =
          new TopMktDataHandler(ticker, type, rateLimiter, topDataLatch, openResultBundles);
      rateLimiter.acquire();
      apiController.reqTopMktData(contracts.get(ticker), "", true, topMktDataHandler);
    }
    if (!topDataLatch.await(2, TimeUnit.MINUTES)) {
      System.out.println("Top market data request times out after 2 minutes");
    }
    System.out.println("\nGot stock results size " + openResultBundles.size());
    return openResultBundles;
  }

  public void subscribeTopMktData(String ticker, ApiController.ITopMktDataHandler h) {
    Contract contract = new Contract();
    contract.symbol(ticker);
    contract.secType("STK");
    if (ticker.equals("CSCO")
        || ticker.equals("CME")
        || ticker.equals("INTC")
        || ticker.equals("MSFT")) contract.primaryExch("NASDAQ");
    contract.exchange("SMART");
    contract.currency("USD");
    // TODO: should be false
    apiController.reqTopMktData(contract, "", false, h);
  }

  @Override
  public void connected() {
    System.out.println("connected");
    isConnected = true;
    connLatch.countDown();
  }

  @Override
  public void disconnected() {
    System.out.println("disconnected");
    isConnected = false;
  }

  @Override
  public void accountList(ArrayList<String> list) {
    for (String a : list) {
      System.out.println("account " + a);
    }
    Util.printColl(list);
  }

  @Override
  public void error(Exception e) {
    logger.error("err: {}", e.getMessage());
  }

  @Override
  public void message(int id, int errorCode, String errorMsg) {
    logger.error("id: {}, err code: {}, err msg: {}", id, errorCode, errorMsg);
    if (id == -1 && (errorCode == 502 // Couldn't connect to TWS.
        // Confirm that API is enabled in TWS via the Configure>API menu command.
        || errorCode == 506 // Unsupported version. For Java clients only.
    )) {
      connLatch.countDown();
    }
  }

  @Override
  public void show(String string) {
    logger.info(string);
  }

  public void scanLowOpenGap(ApiController.IScannerHandler h) {
    ScannerSubscription sub = new ScannerSubscription();
    sub.scanCode("LOW_OPEN_GAP");
    sub.instrument("STK");
    sub.locationCode("STK.US.MAJOR");
    sub.marketCapAbove(5e9);
    sub.numberOfRows(15);
    apiController.reqScannerSubscription(sub, h);
  }

  public void unsubscribeScanner(ApiController.IScannerHandler h) {
    apiController.cancelScannerSubscription(h);
  }
}
