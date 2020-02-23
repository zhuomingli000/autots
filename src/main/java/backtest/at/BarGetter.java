package backtest.at;

import at.feedapi.ATCallback;
import at.feedapi.BarHistoryDbResponseCollection;
import at.feedapi.Helpers;
import at.shared.ATServerAPIDefines;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static backtest.at.ATUtils.systemTime;
import static backtest.at.ActiveTick.apiDefines;

class BarGetter {
  private static final Logger logger = LoggerFactory.getLogger(BarGetter.class);
  private final ActiveTick activeTick;
  private final String ticker;
  private final LocalDate from, to;
  private CountDownLatch latch;
  private List<OHLC> bar;
  private boolean success;
  private AtomicBoolean done;

  BarGetter(ActiveTick activeTick, String ticker, LocalDate from, LocalDate to) {
    this.activeTick = activeTick;
    this.ticker = ticker;
    this.from = from;
    this.to = to;
    done = new AtomicBoolean();
  }
  private class BarHistoryCallback extends ATCallback implements ATCallback.ATBarHistoryResponseCallback {
    @Override
    public void process(long reqId, ATServerAPIDefines.ATBarHistoryResponseType type, BarHistoryDbResponseCollection res) {
      try {
        if (type.m_responseType != ATServerAPIDefines.ATBarHistoryResponseType.BarHistoryResponseSuccess) {
          logger.error("bar history res type: " + type.m_responseType);
          return;
        }
        for (ATServerAPIDefines.ATBARHISTORY_RECORD record : res.GetRecords()) {
          OHLC ohlc = new OHLC();
          ohlc.open = record.open.price;
          ohlc.high = record.high.price;
          ohlc.low = record.low.price;
          ohlc.close = record.close.price;
          ohlc.volume = record.volume;
          ohlc.ticker = ticker;
          ohlc.date = LocalDate.of(record.barTime.year, record.barTime.month, record.barTime.day);
          bar.add(ohlc);
        }
        success = true;
        if (done.get()) {
          logger.error("get bar history of {} from {} to {} after done", ticker, from, to);
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
  boolean fetch() {
    latch = new CountDownLatch(1);
    success = false;
    bar = new ArrayList<>();
    ATServerAPIDefines.SYSTEMTIME beginTime = systemTime(from);
    ATServerAPIDefines.SYSTEMTIME endTime = systemTime(to);
    System.out.println("getting bar history for " + ticker);
    long request =
        activeTick.serverAPI.ATCreateBarHistoryDbRequest(
            activeTick.session,
            Helpers.StringToSymbol(ticker),
            apiDefines.new ATBarHistoryType(ATServerAPIDefines.ATBarHistoryType.BarHistoryDaily),
            (short) 0,
            beginTime,
            endTime,
            new BarHistoryCallback());
    activeTick.serverAPI.ATSendRequest(
        activeTick.session,
        request,
        2900, /* 2.9 seconds */
        new TimeoutCallback());
    try {
      latch.await(3, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } finally {
      done.set(true);
    }
    return success;
  }

  List<OHLC> get() {return bar;}
}
