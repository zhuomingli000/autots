package backtest.at;

import at.shared.ATServerAPIDefines;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class LastQuoteStream {
  private final ConcurrentMap<String, Quote> map;
  public LastQuoteStream(Stream stream) {
    map = new ConcurrentHashMap<>();
    stream.registerQuoteListener("last quote stream", this::onNewQuote);
  }

  private void onNewQuote(ATServerAPIDefines.ATQUOTESTREAM_QUOTE_UPDATE update) {
    Quote quote = new Quote();
    quote.ticker = new String(update.symbol.symbol).trim();
    quote.time = ATUtils.systemTimeToDateTime(update.quoteDateTime);
    quote.askPrice = update.askPrice.price;
    quote.bidPrice = update.bidPrice.price;
    quote.askSize = update.askSize;
    quote.bidSize = update.bidSize;
    map.put(quote.ticker, quote);
  }

  public Map<String, Quote> getQuotes() {
    return map;
  }
}
