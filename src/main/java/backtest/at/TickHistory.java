package backtest.at;

import at.feedapi.ATCallback;
import at.feedapi.Helpers;
import at.feedapi.TickHistoryDbResponseCollection;
import at.shared.ATServerAPIDefines;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static at.shared.ATServerAPIDefines.ATTickHistoryRecordType.TickHistoryRecordQuote;
import static at.shared.ATServerAPIDefines.ATTickHistoryRecordType.TickHistoryRecordTrade;
import static at.shared.ATServerAPIDefines.ATTickHistoryResponseType.*;
import static backtest.at.ATUtils.systemTime;
import static backtest.at.ATUtils.systemTimeToDateTime;

class TickHistory {
  private static final Logger logger = LoggerFactory.getLogger(TickHistory.class);
  private final ActiveTick at;
  private final String ticker;
  private final LocalDateTime from, to;
  private CountDownLatch latch;
  private List<Trade> trades;
  private List<Quote> quotes;
  private boolean success;
  private AtomicBoolean done;

  public TickHistory(ActiveTick at, String ticker, LocalDateTime from, LocalDateTime to) {
    this.at = at;
    this.ticker = ticker;
    this.from = from;
    this.to = to;
    done = new AtomicBoolean();
  }

  private static void logHistoryResponseType(ATServerAPIDefines.ATTickHistoryResponseType type) {
    int t = type.m_responseType;
    if (t == TickHistoryResponseSuccess) {
      logger.info("TickHistoryResponseSuccess");
    } else if (t == TickHistoryResponseDenied) {
      logger.error("TickHistoryResponseDenied");
    } else if (t == TickHistoryResponseInvalidRequest) {
      logger.error("TickHistoryResponseInvalidRequest");
    } else if (t == TickHistoryResponseMaxLimitReached) {
      logger.error("TickHistoryResponseMaxLimitReached");
    } else {
      logger.error("unknown history response type");
    }
  }

  private class TickHistoryCallback extends ATCallback
      implements ATCallback.ATTickHistoryResponseCallback {

    @Override
    public void process(
        long id,
        ATServerAPIDefines.ATTickHistoryResponseType resType,
        TickHistoryDbResponseCollection res) {
      try {
        if (resType.m_responseType != TickHistoryResponseSuccess) {
          logHistoryResponseType(resType);
          return;
        }
        int size = res.GetRecords().size();
        if (size > 30000) {
          logger.info("get {} records", res.GetRecords().size());
        }
        for (ATServerAPIDefines.ATTICKHISTORY_RECORD record : res.GetRecords()) {
          int recordType = record.recordType.m_historyRecordType;
          if (recordType == TickHistoryRecordTrade) {
            ATServerAPIDefines.ATTICKHISTORY_TRADE_RECORD tradeQuote =
                (ATServerAPIDefines.ATTICKHISTORY_TRADE_RECORD) record;
            LocalDateTime time = systemTimeToDateTime(tradeQuote.lastDateTime);
            Trade trade = new Trade();
            trade.price = tradeQuote.lastPrice.price;
            trade.size = tradeQuote.lastSize;
            trade.ticker = ticker;
            trade.time = time;
            trades.add(trade);
          } else if (recordType == TickHistoryRecordQuote) {
            ATServerAPIDefines.ATTICKHISTORY_QUOTE_RECORD quoteRecord =
                (ATServerAPIDefines.ATTICKHISTORY_QUOTE_RECORD) record;
            Quote quote = new Quote();
            quote.ticker = ticker;
            quote.time = systemTimeToDateTime(quoteRecord.quoteDateTime);
            quote.askPrice = quoteRecord.askPrice.price;
            quote.bidPrice = quoteRecord.bidPrice.price;
            quote.askSize = quoteRecord.askSize;
            quote.bidSize = quoteRecord.bidSize;
            quotes.add(quote);
          } else {
            logger.error("{}: neither trade nor quote", ticker);
          }
        }
        success = true;
        if (done.get()) {
          logger.error("get tick history of {} from {} to {} after done", ticker, from, to);
        }
      } finally {
        done.set(true);
        latch.countDown();
      }
    }
  }

  private class TimeoutCallback extends ATCallback implements ATCallback.ATRequestTimeoutCallback {
    @Override
    public void process(long rid) {
      logger.error("tick history request time out");
      done.set(true);
      latch.countDown();
    }
  }

  private boolean get(boolean isTrade, boolean isQuote, int deadline_ms) {
    ATServerAPIDefines.SYSTEMTIME beginTime = systemTime(from);
    ATServerAPIDefines.SYSTEMTIME endTime = systemTime(to);
    latch = new CountDownLatch(1);
    trades = new ArrayList<>();
    quotes = new ArrayList<>();
    success = false;
    long reqId =
        at.serverAPI.ATCreateTickHistoryDbRequest(
            at.session,
            Helpers.StringToSymbol(ticker),
            isTrade,
            isQuote,
            beginTime,
            endTime,
            new TickHistoryCallback());
    at.serverAPI.ATSendRequest(
        at.session,
        reqId,
        deadline_ms, /* 2.9 seconds */
        new TimeoutCallback());
    try {
      latch.await(deadline_ms + 100, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } finally {
      done.set(true);
    }
    return success;
  }

  boolean fetchTrade(int deadline_ms) {
    return get(true, false, deadline_ms);
  }

  boolean fetchQuote(int deadline_ms) {
    return get(false, true, deadline_ms);
  }

  List<Trade> getTrade() {
    return trades;
  }

  List<Quote> getQuote() {
    return quotes;
  }
}
