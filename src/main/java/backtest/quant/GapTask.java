package backtest.quant;

import backtest.at.*;
import backtest.ib.*;
import backtest.io.SQLite;
import backtest.utils.Cal;
import backtest.utils.Email;
import backtest.utils.Task;
import backtest.utils.Util;
import com.google.common.collect.Ordering;
import com.ib.client.Contract;
import com.ib.client.Order;
import com.ib.client.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class GapTask implements Task {
  private static final double stopLoss = 0.02;
  private static final double stopWin = 0.08;
  private static final int maxNumStocksPerDay = 6;
  private static final Logger logger = LoggerFactory.getLogger(GapTask.class);
  private final Email email;
  private final Account account;
  private final IBController ibController;
  private double nequidation;
  private final Orders orders;

  public GapTask(
      Account account, IBController controller, Orders orders, Email email) {
    this.account = account;
    this.orders = orders;
    this.ibController = controller;
    this.email = email;
  }

  @Override
  public void perform(LocalDate date) {
    // 7 a.m.
    logger.info("perform gap task at {}", Util.getEstNow());
    if (!Cal.isBusinessDay(date)) {
      return;
    }

    // get sp500 list.
    List<String> sp500 = Stocks.getSP500Online();
    sp500.remove("GOOG");
    sp500.remove("GOOGL");
    sp500.remove("ZTS");
    while (sp500.size() > 500) {
      logger.error("len > 500. remove: {}", sp500.remove(sp500.size()-1));
    }

    // fetch close price of yesterday.
    Map<String, Double> lastClose = new HashMap<>();
    LocalDate lastBizDay = Cal.getPrevBusinessDay(date);
    try (ActiveTick activeTick = new ActiveTick();
         SQLite ohlcDB = new SQLite("ohlc.db")) {
      BarHistory barHistory = new BarHistory(ohlcDB, activeTick, new DayRanges(ohlcDB));
      for (String ticker : sp500) {
        Optional<OHLC> optOhlc = barHistory.getOrFetch(ticker, lastBizDay, date);
        if (optOhlc.isPresent()) {
          OHLC ohlc = optOhlc.get();
          lastClose.put(ohlc.ticker, ohlc.close);
        }
      }
    } catch (ConnectionException e) {
      logger.info("activetick connection failed");
    }

    for (Map.Entry<String, Double> entry : lastClose.entrySet()) {
      logger.info("close of {}: {}", entry.getKey(), entry.getValue());
    }
    logger.info("lastClose size: {}", lastClose.size());

    ibController.clearPositions(account.getPositions());

    Util.sleepTo(9, 25);
    nequidation = account.getNetLiquid();
    email.reset();

    try (ActiveTick activeTick = new ActiveTick();
         Stream stream = new Stream(activeTick, sp500)) {
      FirstTradeStream firstTradeStream = new FirstTradeStream(stream);
      LastQuoteStream lastQuoteStream = new LastQuoteStream(stream);
      List<Gap> gaps = new ArrayList<>();

      Util.sleepTo(9,30,15);

      // compute gap of all tickers
      for (Trade trade : firstTradeStream.getTrades().values()) {
        if (!lastClose.containsKey(trade.ticker)) {
          email.println(trade.ticker + " does not have last close");
          continue;
        }
        double lastClosePrice = lastClose.get(trade.ticker);
        Gap gap = new Gap();
        gap.ticker = trade.ticker;
        gap.openPrice = trade.price;
        gap.openTime = trade.time;
        gap.lastClose = lastClosePrice;
        gap.gap = (trade.price - lastClosePrice) / lastClosePrice;
        gaps.add(gap);
      }

      Map<String, Quote> lastQuoteMap = lastQuoteStream.getQuotes();
      for (Gap gap : Ordering.natural().leastOf(gaps, maxNumStocksPerDay)) {
        email.println(String.format("gap ticker: %s, gap: %f, first trade price: %f, first trade time: %s, last close price: %f", gap.ticker, gap.gap, gap.openPrice, gap.openTime, gap.lastClose));
        if (lastQuoteMap.containsKey(gap.ticker)) {
          Quote lastQuote = lastQuoteMap.get(gap.ticker);
          double buyPrice = (lastQuote.bidPrice + lastQuote.askPrice) / 2;
          email.println(String.format("will buy %s with %f. last quote: time: %s, bid: %f, ask %f", gap.ticker, buyPrice, lastQuote.time, lastQuote.bidPrice, lastQuote.askPrice));
          buy(createContract(gap.ticker), buyPrice);
        } else {
          email.println(String.format("%s doesn't have any quote yet. Now: %s", gap.ticker, Util.getEstNow()));
        }
      }
    } catch (ConnectionException e) {
      logger.error("activetick connection failed");
    }

    Util.sleepTo(10, 0);
    email.send();
  }

  private Contract createContract(String ticker) {
    Contract contract = new Contract();
    contract.symbol(ticker);
    contract.secType("STK");
    if (ticker.equals("CSCO")
        || ticker.equals("CME")
        || ticker.equals("INTC")
        || ticker.equals("MSFT")) contract.primaryExch("NASDAQ");
    contract.exchange("SMART");
    contract.currency("USD");
    return contract;
  }

  private void buy(Contract contract, double startPrice) {
    logger.info("buy {}", contract.description());
    Order order = new Order();
    order.action("BUY");
    int shares = (int) (nequidation / maxNumStocksPerDay / startPrice);
    if (shares <= 0) {
      email.println(
          "skipped " + contract.symbol() + ". does not have enough fund to buy even one share.");
      return;
    }
    order.totalQuantity(shares);
    order.orderType("LMT");
    order.lmtPrice(convertToTwoDecimal(startPrice));
    order.goodTillDate(
        LocalDate.now().format(DateTimeFormatter.BASIC_ISO_DATE)
        + " 10:00:00 EST");
    order.tif(Types.TimeInForce.GTD);
    OrderHandler orderHandler = new OrderHandler(contract, order, "buy " + contract.symbol());
    orderHandler.addListener("for_stop_and_moc_order_on_fill", this::onFill);
    orderHandler.addListener("for_order_history", orders::record);
    email.println(Util.getEstNow() + ": will buy " + contract.symbol() + " shares: " + shares);
    ibController.placeOrModifyOrder(contract, order, orderHandler);
  }

  private void onFill(FilledState filledState) {
    logger.info("placing stop loss, stop win, and market on close");
    Contract contract = filledState.contract;
    email.println(
        Util.getEstNow()
            + ": "
            + contract.symbol()
            + " filled with "
            + filledState.filledPrice
            + ". filled shares: "
            + filledState.filledShares);

    Order marketOnCloseOrder = new Order();
    marketOnCloseOrder.action("SELL");
    marketOnCloseOrder.orderType("MKT");
    marketOnCloseOrder.goodAfterTime(
        LocalDate.now().format(DateTimeFormatter.BASIC_ISO_DATE) + " 15:59:00 EST");
    marketOnCloseOrder.totalQuantity(filledState.filledShares);
    marketOnCloseOrder.ocaGroup("oca_" + contract.conid() + "_" + contract.symbol());
    marketOnCloseOrder.ocaType(Types.OcaType.CancelWithBlocking);
    OrderHandler marketOnCloseHandler =
        new OrderHandler(
            contract, marketOnCloseOrder, "market on close order of " + contract.symbol());
    marketOnCloseHandler.addListener("for_order_history", orders::record);
    ibController.placeOrModifyOrder(contract, marketOnCloseOrder, marketOnCloseHandler);

//    double stopLossAt = filledState.filledPrice * (1 - stopLoss);
//    Order stopLossOrder = new Order();
//    stopLossOrder.action("SELL");
//    stopLossOrder.orderType("STP LMT");
//    stopLossOrder.totalQuantity(filledState.filledShares);
//    stopLossOrder.lmtPrice(convertToTwoDecimal(stopLossAt));
//    stopLossOrder.auxPrice(convertToTwoDecimal(stopLossAt));
//    stopLossOrder.ocaGroup("oca_" + contract.conid() + "_" + contract.symbol());
//    stopLossOrder.ocaType(Types.OcaType.CancelWithBlocking);
//    OrderHandler stopLossHandler =
//        new OrderHandler(contract, stopLossOrder, "stop loss order of " + contract.symbol());
//    stopLossHandler.addListener("for_order_history", orders::record);
//    ibController.placeOrModifyOrder(contract, stopLossOrder, stopLossHandler);

//    double stopWinAt = filledState.filledPrice * (1 + stopWin);
//    Order stopWinOrder = new Order();
//    stopWinOrder.action("SELL");
//    stopWinOrder.orderType("LMT");
//    stopWinOrder.totalQuantity(filledState.filledShares);
//    stopWinOrder.lmtPrice(convertToTwoDecimal(stopWinAt));
//    stopWinOrder.ocaGroup("oca_" + contract.conid() + "_" + contract.symbol());
//    stopWinOrder.ocaType(Types.OcaType.CancelWithBlocking);
//    OrderHandler stopWinHandler =
//        new OrderHandler(contract, stopWinOrder, "stop win order of " + contract.symbol());
//    stopWinHandler.addListener("for_order_history", orders::record);
//    ibController.placeOrModifyOrder(contract, stopWinOrder, stopWinHandler);
  }

  private double convertToTwoDecimal(double value) {
    BigDecimal bd = new BigDecimal(value);
    bd = bd.setScale(2, RoundingMode.HALF_UP);
    return bd.doubleValue();
  }

  public static void main(String[] args) {
    final Orders orders = new Orders();
    final IBController ibController = new IBController();
    if (!ibController.connect("127.0.0.1", 7497, 4)) return;
    final Account account = new Account(ibController);
    Email email = new Email();
    email.alsoPrintToConsole();
    GapTask task = new GapTask(account, ibController, orders, email);
    task.perform(LocalDate.now());
    ibController.disconnect();
  }
}
