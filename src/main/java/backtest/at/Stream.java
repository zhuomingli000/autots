package backtest.at;

import at.feedapi.ATCallback;
import at.feedapi.Helpers;
import at.feedapi.QuoteStreamResponseCollection;
import at.shared.ATServerAPIDefines;
import backtest.quant.Stocks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class Stream implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(Stream.class);
  private final ConcurrentMap<String, Consumer<ATServerAPIDefines.ATQUOTESTREAM_QUOTE_UPDATE>>
      quoteListeners;
  private final ConcurrentMap<String, Consumer<ATServerAPIDefines.ATQUOTESTREAM_TRADE_UPDATE>>
      tradeListeners;
  private final List<ATServerAPIDefines.ATSYMBOL> list;
  private final ActiveTick activeTick;

  public Stream(ActiveTick activeTick, List<String> toQuery) {
    this.activeTick = activeTick;
    quoteListeners = new ConcurrentHashMap<>();
    tradeListeners = new ConcurrentHashMap<>();
    activeTick.serverAPI.ATSetStreamUpdateCallback(activeTick.session, new StreamUpdateCallback());
    while (toQuery.size() > 500) {
      logger.error("len > 500. remove: {}", toQuery.remove(toQuery.size() - 1));
    }
    list = toQuery.stream().map(Helpers::StringToSymbol).collect(Collectors.toList());
    ATServerAPIDefines.ATStreamRequestType requestType =
        ActiveTick.apiDefines.new ATStreamRequestType();
    requestType.m_streamRequestType = ATServerAPIDefines.ATStreamRequestType.StreamRequestSubscribe;
    long reqId =
        activeTick.serverAPI.ATCreateQuoteStreamRequest(
            activeTick.session, list, requestType, new StreamStatusCallback());
    activeTick.serverAPI.ATSendRequest(
        activeTick.session,
        reqId,
        3000,
        new TimeoutCallback("activetick stream subscribe request time out"));
  }

  // unsubscribe
  @Override
  public void close() {
    ATServerAPIDefines.ATStreamRequestType requestType =
        ActiveTick.apiDefines.new ATStreamRequestType();
    requestType.m_streamRequestType =
        ATServerAPIDefines.ATStreamRequestType.StreamRequestUnsubscribe;
    long reqId =
        activeTick.serverAPI.ATCreateQuoteStreamRequest(
            activeTick.session, list, requestType, new StreamStatusCallback());
    activeTick.serverAPI.ATSendRequest(
        activeTick.session,
        reqId,
        3000,
        new TimeoutCallback("active stream unsubscribe request time out"));
    removeAllListener();
  }

  private static final class StreamStatusCallback extends ATCallback
      implements ATCallback.ATQuoteStreamResponseCallback {
    @Override
    public void process(
        long l, ATServerAPIDefines.ATStreamResponseType type, QuoteStreamResponseCollection res) {
      if (type.m_responseType != ATServerAPIDefines.ATStreamResponseType.StreamResponseSuccess) {
        logger.error("stream req failed: res type: {}", type.m_responseType);
      } else {
        res.GetItems()
            .stream()
            .filter(
                item ->
                    item.symbolStatus.m_atSymbolStatus
                        != ATServerAPIDefines.ATSymbolStatus.SymbolStatusSuccess)
            .forEach(
                item ->
                    logger.error(
                        "symbol failed in stream req; status: {}; symbol: {}",
                        item.symbolStatus.m_atSymbolStatus,
                        new String(item.symbol.symbol)));
      }
    }
  }

  private static final class TimeoutCallback extends ATCallback
      implements ATCallback.ATRequestTimeoutCallback {
    private final String msg;

    TimeoutCallback(String msg) {
      this.msg = msg;
    }

    @Override
    public void process(long rid) {
      logger.error(msg);
    }
  }

  private final class StreamUpdateCallback extends ATCallback
      implements ATCallback.ATStreamUpdateCallback {
    @Override
    public void process(ATServerAPIDefines.ATSTREAM_UPDATE update) {
      if (update.updateType.GetUpdateType()
          == ATServerAPIDefines.ATStreamUpdateType.StreamUpdateQuote) {
        ATServerAPIDefines.ATQUOTESTREAM_QUOTE_UPDATE quoteUpdate =
            (ATServerAPIDefines.ATQUOTESTREAM_QUOTE_UPDATE) update;
        for (Consumer<ATServerAPIDefines.ATQUOTESTREAM_QUOTE_UPDATE> consumer :
            quoteListeners.values()) {
          consumer.accept(quoteUpdate);
        }
      } else if (update.updateType.GetUpdateType()
          == ATServerAPIDefines.ATStreamUpdateType.StreamUpdateTrade) {
        ATServerAPIDefines.ATQUOTESTREAM_TRADE_UPDATE tradeUpdate =
            (ATServerAPIDefines.ATQUOTESTREAM_TRADE_UPDATE) update;
        for (Consumer<ATServerAPIDefines.ATQUOTESTREAM_TRADE_UPDATE> consumer :
            tradeListeners.values()) {
          consumer.accept(tradeUpdate);
        }
      } else if (update.updateType.GetUpdateType()
          == ATServerAPIDefines.ATStreamUpdateType.StreamUpdateRefresh) {
        logger.info("get refresh update");
      } else if (update.updateType.GetUpdateType()
          == ATServerAPIDefines.ATStreamUpdateType.StreamUpdateTopMarketMovers) {
        logger.info("get market mover update");
      }
    }
  }

  void registerQuoteListener(
      String name, Consumer<ATServerAPIDefines.ATQUOTESTREAM_QUOTE_UPDATE> listener) {
    quoteListeners.put(name, listener);
  }

  void registerTradeListener(
      String name, Consumer<ATServerAPIDefines.ATQUOTESTREAM_TRADE_UPDATE> listener) {
    tradeListeners.put(name, listener);
  }

  private void removeAllListener() {
    quoteListeners.clear();
    tradeListeners.clear();
  }
}
