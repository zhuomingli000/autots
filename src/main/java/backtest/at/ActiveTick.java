package backtest.at;

import at.feedapi.*;
import at.shared.ATServerAPIDefines;
import at.shared.ATServerAPIDefines.ATBarHistoryResponseType;
import backtest.quant.Stocks;
import backtest.utils.*;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static at.shared.ATServerAPIDefines.ATQuoteDbResponseType.*;
import static at.shared.ATServerAPIDefines.ATQuoteFieldType.AskPrice;
import static at.shared.ATServerAPIDefines.ATQuoteFieldType.OpenPrice;
import static at.shared.ATServerAPIDefines.ATSymbolStatus.*;
import static at.shared.ATServerAPIDefines.ATTickHistoryRecordType.TickHistoryRecordQuote;
import static at.shared.ATServerAPIDefines.ATTickHistoryRecordType.TickHistoryRecordTrade;
import static at.shared.ATServerAPIDefines.ATTickHistoryResponseType.*;

public class ActiveTick implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(ActiveTick.class);
  final ActiveTickServerAPI serverAPI;
  private boolean inited;
  private boolean connected;
  Session session;
  static final ATServerAPIDefines apiDefines = new ATServerAPIDefines();
  private static final String quotesPath = "data/quotes-160724";
  private static final String tradesPath = "data/trades-160724";
  private static final String ohlcPath ="data/ohlc-160724";
  private final Object quoteLock = new Object();
  private final Object tradeLock = new Object();
  private TimedSemaphore<Label> latch;

  private final Map<LocalDate, Set<String>> tradeDone = new ConcurrentHashMap<>();
  private final Map<LocalDate, Map<LocalTime, Integer>> tradeDoneNum = new ConcurrentHashMap<>();

  public ActiveTick() throws ConnectionException {
    inited = false;
    connected = false;
    serverAPI = new ActiveTickServerAPI();
    if (!serverAPI.ATInitAPI()) return;
    session = serverAPI.ATCreateSession();
    ATServerAPIDefines.ATGUID apiKey = apiDefines.new ATGUID();
    apiKey.SetGuid("6b379b465bd243209d0b572d42479645");
    serverAPI.ATSetAPIKey(session, apiKey);
//    serverAPI.ATSetStreamUpdateCallback(session, update -> {});
    String primaryHostname = "activetick1.activetick.com";
    String backupHostname = "activetick2.activetick.com";
    int port = 443;
    CountDownLatch sessionInitLatch = new CountDownLatch(1);
    serverAPI.ATInitSession(
        session,
        primaryHostname,
        backupHostname,
        port,
        (s, sessionStatusType) -> {
          inited = false;
          switch (sessionStatusType.m_atSessionStatusType) {
            case ATServerAPIDefines.ATSessionStatusType.SessionStatusConnected:
              if (!inited) {
                logger.info("SessionStatusConnected");
                inited = true;
                sessionInitLatch.countDown();
              }
              break;
            case ATServerAPIDefines.ATSessionStatusType.SessionStatusDisconnected:
              logger.info("SessionStatusDisconnected");
              break;
            case ATServerAPIDefines.ATSessionStatusType.SessionStatusDisconnectedDuplicateLogin:
              logger.info("SessionStatusDisconnectedDuplicateLogin");
              break;
            case ATServerAPIDefines.ATSessionStatusType.SessionStatusDisconnectedInactivity:
              logger.info("SessionStatusDisconnectedInactivity");
              break;
            default:
              logger.info("unknown sessionStatusType");
              break;
          }
        });
    try {
      sessionInitLatch.await(5, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    connect();
  }

  private void disconnect() {
    if (connected) {
      serverAPI.ATShutdownSession(session);
      serverAPI.ATShutdownAPI();
    }
  }

  @Override
  public void close() {
    disconnect();
  }

  private static abstract class BarHistoryCallback extends ATCallback
      implements ATCallback.ATBarHistoryResponseCallback {}

  private static abstract class LoginCallback extends ATCallback
      implements ATCallback.ATLoginResponseCallback {}

  private static abstract class TimeoutCallback extends ATCallback
      implements ATCallback.ATRequestTimeoutCallback {}

  private static abstract class TickHistoryCallback extends ATCallback
      implements ATCallback.ATTickHistoryResponseCallback {}

  private static abstract class QuoteResponseCallback extends ATCallback
      implements ATCallback.ATQuoteDbResponseCallback {}

  private boolean connect() throws ConnectionException {
    if (!inited) throw new ConnectionException("session initialization failed.");
    String username = "xyyk";
    String password = "DHDjPQajE2Db";
    CountDownLatch connLatch = new CountDownLatch(1);
    long requestId =
        serverAPI.ATCreateLoginRequest(
            session,
            username,
            password,
            new LoginCallback() {
              @Override
              public void process(
                  Session session, long reqId, ATServerAPIDefines.ATLOGIN_RESPONSE res) {
                String strLoginResponseType;
                connected = false;
                switch (res.loginResponse.m_atLoginResponseType) {
                  case ATServerAPIDefines.ATLoginResponseType.LoginResponseSuccess:
                    strLoginResponseType = "LoginResponseSuccess";
                    connected = true;
                    break;
                  case ATServerAPIDefines.ATLoginResponseType.LoginResponseInvalidUserid:
                    strLoginResponseType = "LoginResponseInvalidUserid";
                    break;
                  case ATServerAPIDefines.ATLoginResponseType.LoginResponseInvalidPassword:
                    strLoginResponseType = "LoginResponseInvalidPassword";
                    break;
                  case ATServerAPIDefines.ATLoginResponseType.LoginResponseInvalidRequest:
                    strLoginResponseType = "LoginResponseInvalidRequest";
                    break;
                  case ATServerAPIDefines.ATLoginResponseType.LoginResponseLoginDenied:
                    strLoginResponseType = "LoginResponseLoginDenied";
                    break;
                  case ATServerAPIDefines.ATLoginResponseType.LoginResponseServerError:
                    strLoginResponseType = "LoginResponseServerError";
                    break;
                  default:
                    strLoginResponseType = "unknown";
                    break;
                }
                logger.info(strLoginResponseType);
                connLatch.countDown();
              }
            });
    int timeoutMillis = 3000;
    serverAPI.ATSendRequest(
        session,
        requestId,
        timeoutMillis,
        new TimeoutCallback() {
          @Override
          public void process(long reqId) {
            connLatch.countDown();
          }
        });
    try {
      connLatch.await(timeoutMillis + 1000, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    if (connected) {
      return true;
    } else {
      throw new ConnectionException("Failed to connect to ActiveTick");
    }
  }

  private ATServerAPIDefines.SYSTEMTIME systemTime(LocalDateTime date) {
    ATServerAPIDefines.SYSTEMTIME systemTime = apiDefines.new SYSTEMTIME();
    systemTime.year = (short) date.getYear();
    systemTime.month = (short) date.getMonthValue();
    systemTime.day = (short) date.getDayOfMonth();
    systemTime.hour = (short) date.getHour();
    systemTime.minute = (short) date.getMinute();
    systemTime.second = (short) date.getSecond();
    return systemTime;
  }

  private LocalDateTime systemTimeToDateTime(ATServerAPIDefines.SYSTEMTIME systemTime) {
    return LocalDateTime.of(
        systemTime.year,
        systemTime.month,
        systemTime.day,
        systemTime.hour,
        systemTime.minute,
        systemTime.second);
  }

  public Map<String, Double> getOpenPrice(List<String> tickers) {
    Map<String, Double> ret = new HashMap<>();
    System.out.println("tickers size: " + tickers.size());
    if (tickers.size() > 500) {
      logger.error("cannot get more than 500 tickers");
      return ret;
    }
    List<ATServerAPIDefines.ATSYMBOL> list =
        tickers.stream().map(Helpers::StringToSymbol).collect(Collectors.toList());
    List<ATServerAPIDefines.ATQuoteFieldType> types =
        ImmutableList.of(
            apiDefines.new ATQuoteFieldType(ATServerAPIDefines.ATQuoteFieldType.OpenPrice),
            apiDefines.new ATQuoteFieldType(AskPrice));
    CountDownLatch latch = new CountDownLatch(1);
    long reqId =
        serverAPI.ATCreateQuoteDbRequest(
            session,
            list,
            types,
            new QuoteResponseCallback() {
              @Override
              public void process(
                  long id,
                  ATServerAPIDefines.ATQuoteDbResponseType resType,
                  QuoteDbResponseCollection res) {
                if (resType.m_atQuoteDbResponseType != QuoteDbResponseSuccess) {
                  logQuoteDbResType(resType.m_atQuoteDbResponseType);
                  return;
                }
                logger.error("got {} response items", res.GetItems().size());
                for (ATServerAPIDefines.QuoteDbResponseItem responseItem : res.GetItems()) {
                  if (responseItem.m_atResponse.status.m_atSymbolStatus != SymbolStatusSuccess) {
                    logSymbolStatus(responseItem.m_atResponse.status.m_atSymbolStatus);
                    continue;
                  }
                  String strItemSymbol = new String(responseItem.m_atResponse.symbol.symbol);
                  int plainItemSymbolIndex = strItemSymbol.indexOf((byte) 0);
                  strItemSymbol = strItemSymbol.substring(0, plainItemSymbolIndex);
                  for (ATServerAPIDefines.QuoteDbDataItem dataItem : responseItem.m_vecDataItems) {
                    if (dataItem.m_dataItem.fieldStatus.m_atFieldStatus
                        != ATServerAPIDefines.ATFieldStatus.Success) {
                      logFieldStatus(dataItem.m_dataItem.fieldStatus.m_atFieldStatus);
                      continue;
                    }
                    if (dataItem.m_dataItem.dataType.m_atDataType
                        != ATServerAPIDefines.ATDataType.Price) {
                      logDataType(dataItem.m_dataItem.dataType.m_atDataType);
                      continue;
                    }
                    ATServerAPIDefines.ATPRICE price = Helpers.BytesToPrice(dataItem.GetItemData());
                    if (price.price > 0.1) {
                      if (dataItem.m_dataItem.fieldType.m_atQuoteFieldType == AskPrice) {
                        ret.putIfAbsent(strItemSymbol, price.price);
                      } else if (dataItem.m_dataItem.fieldType.m_atQuoteFieldType == OpenPrice) {
                        ret.put(strItemSymbol, price.price);
                      } else {
                        logger.info(
                            "quote field type: {}",
                            dataItem.m_dataItem.fieldType.m_atQuoteFieldType);
                      }
                    }
                  }
                }
                latch.countDown();
              }
            });
    serverAPI.ATSendRequest(
        session,
        reqId,
        20000,
        new TimeoutCallback() {
          @Override
          public void process(long l) {
            logger.error("timeout");
            latch.countDown();
          }
        });
    try {
      if (!latch.await(30, TimeUnit.SECONDS)) {
        logger.error("quote db request time out");
      }
    } catch (InterruptedException e) {
      logger.error("interrupted");
      Thread.currentThread().interrupt();
    }
    logger.error("ret.size() = {}; want: {}", ret.size(), tickers.size());
    if (ret.size() != tickers.size()) {
      tickers
          .stream()
          .filter(ticker -> !ret.containsKey(ticker))
          .forEach(ticker -> logger.error("{} is not contained", ticker));
    }
    return ret;
  }

  private void logQuoteDbResType(int type) {
    if (type == QuoteDbResponseSuccess) {
      logger.info("QuoteDbResponseSuccess");
    } else if (type == QuoteDbResponseInvalidRequest) {
      logger.info("QuoteDbResponseInvalidRequest");
    } else if (type == QuoteDbResponseDenied) {
      logger.info("QuoteDbResponseDenied");
    } else if (type == QuoteDbResponseUnavailable) {
      logger.info("QuoteDbResponseUnavailable");
    } else {
      logger.info("quote db response type unknown");
    }
  }

  private void logSymbolStatus(byte type) {
    if (type == SymbolStatusSuccess) {
      logger.info("SymbolStatusSuccess");
    } else if (type == SymbolStatusInvalid) {
      logger.info("SymbolStatusInvalid");
    } else if (type == SymbolStatusNoPermission) {
      logger.info("SymbolStatusNoPermission");
    } else if (type == SymbolStatusUnavailable) {
      logger.info("SymbolStatusUnavailable");
    } else {
      logger.info("symbol status unknown");
    }
  }

  private void logFieldStatus(int type) {
    if (type == ATServerAPIDefines.ATFieldStatus.Success) {
      logger.info("Success");
    } else if (type == ATServerAPIDefines.ATFieldStatus.Denied) {
      logger.info("Denied");
    } else if (type == ATServerAPIDefines.ATFieldStatus.Invalid) {
      logger.info("Invalid");
    } else if (type == ATServerAPIDefines.ATFieldStatus.Unavailable) {
      logger.info("Unavailable");
    } else {
      logger.info("field status unknown");
    }
  }

  private void logDataType(int type) {
    if (type == ATServerAPIDefines.ATDataType.Byte) {
      logger.info("got byte");
    } else if (type == ATServerAPIDefines.ATDataType.ByteArray) {
      logger.info("got byte array");
    } else if (type == ATServerAPIDefines.ATDataType.UInteger32) {
      logger.info("uint32");
    } else if (type == ATServerAPIDefines.ATDataType.UInteger64) {
      logger.info("got uint64");
    } else if (type == ATServerAPIDefines.ATDataType.Integer32) {
      logger.info("got int32");
    } else if (type == ATServerAPIDefines.ATDataType.Integer64) {
      logger.info("got int64");
    } else if (type == ATServerAPIDefines.ATDataType.Price) {
      logger.info("got price");
    } else if (type == ATServerAPIDefines.ATDataType.String) {
      logger.info("got string");
    } else if (type == ATServerAPIDefines.ATDataType.UnicodeString) {
      logger.info("got unicode string");
    } else if (type == ATServerAPIDefines.ATDataType.DateTime) {
      logger.info("got datetime");
    } else {
      logger.info("data type unknown");
    }
  }

  private void logHistoryResponseType(ATServerAPIDefines.ATTickHistoryResponseType type) {
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

  private void saveQuote(ATServerAPIDefines.ATTICKHISTORY_RECORD record, String ticker) {
    synchronized (quoteLock) {
      ATServerAPIDefines.ATTICKHISTORY_QUOTE_RECORD quoteRecord =
          (ATServerAPIDefines.ATTICKHISTORY_QUOTE_RECORD) record;
      LocalDateTime time = systemTimeToDateTime(quoteRecord.quoteDateTime);
      String output =
          String.format(
              "%s %s %s %s %s %s\n",
              time,
              ticker,
              quoteRecord.askPrice.price,
              quoteRecord.askSize,
              quoteRecord.bidPrice.price,
              quoteRecord.bidSize);
      try {
        Files.write(
            Paths.get(quotesPath),
            output.getBytes(),
            StandardOpenOption.CREATE,
            StandardOpenOption.APPEND);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  private void saveTrade(ATServerAPIDefines.ATTICKHISTORY_RECORD record, String ticker) {
    synchronized (tradeLock) {
      ATServerAPIDefines.ATTICKHISTORY_TRADE_RECORD tradeQuote =
          (ATServerAPIDefines.ATTICKHISTORY_TRADE_RECORD) record;
      LocalDateTime time = systemTimeToDateTime(tradeQuote.lastDateTime);
      String output =
          String.format(
              "%s %s %s %s\n", time, ticker, tradeQuote.lastPrice.price, tradeQuote.lastSize);
      try {
        Files.write(
            Paths.get(tradesPath),
            output.getBytes(),
            StandardOpenOption.CREATE,
            StandardOpenOption.APPEND);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  private void getTickHistory(LocalDateTime from, LocalDateTime to, String ticker) {
    Label label = new Label();
    label.ticker = ticker;
    label.from = from;
    label.to = to;
    latch.register(label);
    ATServerAPIDefines.SYSTEMTIME beginTime = systemTime(from);
    ATServerAPIDefines.SYSTEMTIME endTime = systemTime(to);
    long reqId =
        serverAPI.ATCreateTickHistoryDbRequest(
            session,
            Helpers.StringToSymbol(ticker),
            true, // is trade
            false, // is quote
            beginTime,
            endTime,
            new TickHistoryCallback() {
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
                  if (size > 20000) {
                    logger.info("get {} records", res.GetRecords().size());
                  }
                  for (ATServerAPIDefines.ATTICKHISTORY_RECORD record : res.GetRecords()) {
                    int recordType = record.recordType.m_historyRecordType;
                    if (recordType == TickHistoryRecordTrade) {

                      ATServerAPIDefines.ATTICKHISTORY_TRADE_RECORD tradeQuote =
                          (ATServerAPIDefines.ATTICKHISTORY_TRADE_RECORD) record;
                      LocalDateTime time = systemTimeToDateTime(tradeQuote.lastDateTime);

                      Set<String> doneSet = tradeDone.getOrDefault(time.toLocalDate(), new HashSet<>());
                      doneSet.add(ticker);
                      tradeDone.put(time.toLocalDate(), doneSet);
                      Map<LocalTime, Integer> tradeDoneNumMap = tradeDoneNum.getOrDefault(time.toLocalDate(), new HashMap<>());
                      System.out.println(time);
                      if (time.toLocalTime().isBefore(LocalTime.of(9,31)))
                        tradeDoneNumMap.merge(LocalTime.of(9,31), 1, Integer::sum);
                      if (time.toLocalTime().isBefore(LocalTime.of(9,32)))
                        tradeDoneNumMap.merge(LocalTime.of(9,32), 1, Integer::sum);
                      if (time.toLocalTime().isBefore(LocalTime.of(9,35)))
                        tradeDoneNumMap.merge(LocalTime.of(9,35), 1, Integer::sum);
                      if (time.toLocalTime().isBefore(LocalTime.of(9,40)))
                        tradeDoneNumMap.merge(LocalTime.of(9,40), 1, Integer::sum);
                      if (time.toLocalTime().isBefore(LocalTime.of(9,45)))
                        tradeDoneNumMap.merge(LocalTime.of(9,45), 1, Integer::sum);
                      if (time.toLocalTime().isBefore(LocalTime.of(9,50)))
                        tradeDoneNumMap.merge(LocalTime.of(9,50), 1, Integer::sum);
                      if (time.toLocalTime().isBefore(LocalTime.of(9,55)))
                        tradeDoneNumMap.merge(LocalTime.of(9,55), 1, Integer::sum);
                      if (time.toLocalTime().isBefore(LocalTime.of(10,0)))
                        tradeDoneNumMap.merge(LocalTime.of(10,0), 1, Integer::sum);
                      tradeDoneNum.put(time.toLocalDate(), tradeDoneNumMap);

                      String output =
                          String.format(
                              "%s %s %s %s\n", time, ticker, tradeQuote.lastPrice.price, tradeQuote.lastSize);
                      try {
                        Files.write(
                            Paths.get("data/first-trade"),
                            output.getBytes(),
                            StandardOpenOption.CREATE,
                            StandardOpenOption.APPEND);
                      } catch (IOException e) {
                        e.printStackTrace();
                      }

                      break;

                    } else if (recordType == TickHistoryRecordQuote) {
                      saveQuote(record, ticker);
                    } else {
                      logger.error("{}: neither trade nor quote", ticker);
                    }
                  }
                } finally {
                  latch.arrive(label);
                }
              }
            });
    serverAPI.ATSendRequest(
        session,
        reqId,
        2900, /* 2.9 seconds */
        new TimeoutCallback() {
          @Override
          public void process(long rid) {
            logger.error("tick history request time out");
            latch.arrive(label);
          }
        });
  }

  private void getBarHistory(
      LocalDateTime from,
      LocalDateTime to,
      String ticker) {
    Label label = new Label();
    label.ticker = ticker;
    label.from = from;
    label.to = to;
    latch.register(label);
    ATServerAPIDefines.SYSTEMTIME beginTime = systemTime(from);
    ATServerAPIDefines.SYSTEMTIME endTime = systemTime(to);
    System.out.println("getting bar history for " + ticker);
    long request =
        serverAPI.ATCreateBarHistoryDbRequest(
            session,
            Helpers.StringToSymbol(ticker),
            apiDefines.new ATBarHistoryType(ATServerAPIDefines.ATBarHistoryType.BarHistoryDaily),
            (short) 0,
            beginTime,
            endTime,
            new BarHistoryCallback() {
              @Override
              public void process(
                  long id, ATBarHistoryResponseType resType, BarHistoryDbResponseCollection res) {
                try {
                  if (resType.m_responseType != ATBarHistoryResponseType.BarHistoryResponseSuccess) {
                    System.out.println("bar history res type: " + resType);
                    // TODO(yukang): handle res type error and time out callback error in latch.
                    return;
                  }
                  for (ATServerAPIDefines.ATBARHISTORY_RECORD record : res.GetRecords()) {
                    String result =
                        String.format(
                            "%s %d-%d-%d %f %f %f %f",
                            ticker,
                            record.barTime.year,
                            record.barTime.month,
                            record.barTime.day,
                            record.open.price,
                            record.high.price,
                            record.low.price,
                            record.close.price);
                    try {
                      Files.write(
                          Paths.get(ohlcPath),
                          String.format("%s\n", result).getBytes(),
                          StandardOpenOption.CREATE,
                          StandardOpenOption.APPEND);
                    } catch (IOException e) {
                      e.printStackTrace();
                    }
                  }
                } finally {
                  latch.arrive(label);
                }
              }
            });
    serverAPI.ATSendRequest(
        session,
        request,
        2900, /* 2.9 seconds */
        new TimeoutCallback() {
          @Override
          public void process(long rid) {
            logger.error("bar history request time out");
            latch.arrive(label);
          }
        });
  }

  private void getMorningTick(String ticker, LocalDateTime from, LocalDateTime end) {
    Duration duration = Duration.ofMinutes(5);
    for (LocalDateTime time = from; !time.isAfter(end); time = time.plus(duration)) {
      getTickHistory(time, time.plus(duration), ticker);
    }
  }

  private void getFirstTradeTime(List<String> tickers, LocalDate from, LocalDate to) {
    LocalTime morningStart = LocalTime.of(9,30);
    LocalTime morningEnd = LocalTime.of(10,0);
    for (LocalDate date = Cal.getLatestBusinessDayAfter(from);
         date.isBefore(to);
         date = Cal.getNextBusinessDay(date)) {
      Stopwatch stopwatch = Stopwatch.createStarted();
      for (String ticker : tickers) {
        Set<Label> timeoutSet;
        int retry = 0;
        do {
          retry++;
          latch = new TimedSemaphore<>(1, Duration.ofSeconds(5));
          getTickHistory(LocalDateTime.of(date, morningStart), LocalDateTime.of(date, morningEnd), ticker);
          latch.await();
          timeoutSet = latch.getTimeoutSet();
        } while (timeoutSet.size() != 0 && retry <= 10);
      }
      logger.info("{} done in {}", date, stopwatch);
      logger.info("{}", Util.map2str(tradeDoneNum.get(date)));
    }
  }

  private synchronized void getTickHistory(List<String> tickers, LocalDate from, LocalDate to) {
    System.out.println("get tick history");
    LocalTime morningStart = LocalTime.of(9,30);
    LocalTime morningEnd = LocalTime.of(10,0);
    LocalTime afternoonStart = LocalTime.of(15,59);
    LocalTime afternoonEnd = LocalTime.of(16,0);
    for (LocalDate date = Cal.getLatestBusinessDayAfter(from);
        date.isBefore(to);
        date = Cal.getNextBusinessDay(date)) {
      latch = new TimedSemaphore<>(5, Duration.ofSeconds(5));
      Stopwatch watch = Stopwatch.createStarted();
      for (String ticker : tickers) {
        getMorningTick(ticker, LocalDateTime.of(date, morningStart), LocalDateTime.of(date, morningEnd));
        getTickHistory(LocalDateTime.of(date, afternoonStart), LocalDateTime.of(date, afternoonEnd), ticker);
      }
      latch.await();
      Set<Label> timeoutSet = latch.getTimeoutSet();
      System.out.println("timeout set size: " + timeoutSet.size());
      int retry = 1;
      while (!timeoutSet.isEmpty() && retry < 10) {
        // at most 5 concurrent requests.
        int npermit = Math.min(timeoutSet.size() / 10 + 1, 5);
        latch = new TimedSemaphore<>(npermit, Duration.ofSeconds(5));
        for (Label label : timeoutSet) {
          getTickHistory(label.from, label.to, label.ticker);
        }
        latch.await();
        timeoutSet = latch.getTimeoutSet();
        retry++;
      }
      if (!timeoutSet.isEmpty()) {
        logger.error("{} still times out after 10 retry");
      }
      logger.info("{}: {}", date, watch);
    }
  }

  // Gets history in [from, to]
  private void getBarHistory(List<String> tickers, LocalDate from, LocalDate to) {
    System.out.println("get bar history");
    // Only date matters. Time doesn't affect so 23:59 is the same as 0:0.
    // Both begin and end are included.
    LocalDateTime beginTime = LocalDateTime.of(from, LocalTime.of(0,0));
    LocalDateTime endTime = LocalDateTime.of(to, LocalTime.of(23,59,59));
    latch = new TimedSemaphore<>(5, Duration.ofSeconds(5));
    for (String ticker : tickers) {
      getBarHistory(beginTime, endTime, ticker);
    }
    latch.await();

    Set<Label> timeoutSet = latch.getTimeoutSet();
    System.out.println("timeout set size: " + timeoutSet.size());
    int retry = 1;
    while (!timeoutSet.isEmpty() && retry < 10) {
      int npermit = Math.min(timeoutSet.size() / 10 + 1, 5);
      logger.info("retry " + timeoutSet.size() + " items with " + npermit + " permits");
      latch = new TimedSemaphore<>(npermit, Duration.ofSeconds(5));
      for (Label label : timeoutSet) {
        getBarHistory(label.from, label.to, label.ticker);
      }
      latch.await();
      timeoutSet = latch.getTimeoutSet();
      retry++;
    }
    if (!timeoutSet.isEmpty()) {
      logger.error("{} still times out after 10 retry");
    }
  }

  public static void main(String[] args) {
    System.out.println("start");
    Stocks stocks = new Stocks();
    LocalDate from = LocalDate.of(2016, 5, 20);
    LocalDate to = LocalDate.of(2016, 7, 23);
    List<String> tickers = stocks.getSP500();

    try (ActiveTick activeTick = new ActiveTick()) {
//    activeTick.getBarHistory(tickers, from, to);
      activeTick.getFirstTradeTime(tickers, from, to);
    } catch (ConnectionException e) {
      logger.info("connection fails");
    }
  }
}
