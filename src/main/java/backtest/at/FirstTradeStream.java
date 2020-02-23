package backtest.at;

import at.shared.ATServerAPIDefines;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class FirstTradeStream {
  private static final LocalTime openTime = LocalTime.of(9,30);
  private final ConcurrentMap<String, Trade> map;

  public FirstTradeStream(Stream stream) {
    map = new ConcurrentHashMap<>();
    stream.registerTradeListener("first trade listner", this::onNewTrade);
  }

  private synchronized void onNewTrade(ATServerAPIDefines.ATQUOTESTREAM_TRADE_UPDATE update) {
    String ticker = new String(update.symbol.symbol).trim();
    LocalDateTime time = ATUtils.systemTimeToDateTime(update.lastDateTime);
    if (time.toLocalTime().isBefore(openTime)) return;
    if (map.containsKey(ticker)) {
      if (map.get(ticker).time.isAfter(time)) {
        insert(ticker, time, update);
      }
    } else {
      insert(ticker, time, update);
    }
  }

  private void insert(String ticker, LocalDateTime time, ATServerAPIDefines.ATQUOTESTREAM_TRADE_UPDATE update) {
    Trade trade = new Trade();
    trade.ticker = ticker;
    trade.time = time;
    trade.price = update.lastPrice.price;
    trade.size = update.lastSize;
    map.put(ticker, trade);
  }

  public Map<String, Trade> getTrades() {
    return map;
  }
}
