package backtest.at;

import backtest.io.SQLite;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.Optional;

public class LastQuotes {
  private static final Logger logger = LoggerFactory.getLogger(Quote.class);
  private final SQLite sqlite;
  private final ActiveTick activeTick;
  private final TimeRanges ranges;
  public LastQuotes(SQLite sqlite, ActiveTick activeTick, TimeRanges ranges) {
    this.sqlite = sqlite;
    this.activeTick = activeTick;
    this.ranges = ranges;
    createTableIfNotExists();
  }

  private void createTableIfNotExists(){
    String create = "create table if not exists quotes (ticker text not null, time int not null, ask_price double not null, ask_size int not null, bid_price double not null, bid_size int not null)";
    String index = "create unique index if not exists index_last_quotes_ticker_time on quotes (ticker, time)";
    sqlite.executeUpdate(ImmutableList.of(create, index));
  }

  private Optional<Quote> getLastQuoteBefore(String ticker, LocalDate date, LocalTime time) {
    LocalDateTime from = LocalDateTime.of(date, LocalTime.of(9,30));
    LocalDateTime to = LocalDateTime.of(date, time);
    String sql =
        String.format(
            "select strftime('%%Y-%%m-%%dT%%H:%%M:%%S', time,'unixepoch') as time, ask_price, ask_size, bid_price, bid_size from quotes where ticker='%s' and time>=strftime('%%s','%s') and time<strftime('%%s','%s') order by time desc limit 1",
            ticker,
            from,
            to);
    try (ResultSet resultSet = sqlite.get(sql)) {
      if (resultSet.next()) {
        Quote quote = new Quote();
        quote.ticker = ticker;
        quote.time = LocalDateTime.parse(resultSet.getString("time"));
        quote.askPrice = resultSet.getDouble("ask_price");
        quote.bidPrice = resultSet.getDouble("bid_price");
        quote.askSize = resultSet.getLong("ask_size");
        quote.bidSize = resultSet.getLong("bid_size");
        return Optional.of(quote);
      } else {
//        logger.info("result does not exist in sqlite. sql: {}", sql);
      }
    } catch (SQLException e) {
      logger.error("err in Trade::get: {}", e.getMessage());
    }
    return Optional.empty();
  }

  private List<Quote> fetchFromActiveTick(String ticker, LocalDateTime from, LocalDateTime to) {
    final int maxRetry = 3;
    int i = 0;
    boolean success;
    int deadlineMs = 3000; // in milliseconds
    logger.info("fetch quotes of {} from {} to {}", ticker, from, to);
    do {
      TickHistory tickHistory = new TickHistory(activeTick, ticker, from, to);
      success = tickHistory.fetchQuote(deadlineMs * (i+1));
      if (i>0) logger.error("trial number: {}", i);
      if (success && !tickHistory.getQuote().isEmpty()) {
        return tickHistory.getQuote();
      }
      i++;
    } while (i < maxRetry);
    return ImmutableList.of();
  }

  public Optional<Quote> getOrFetchLastQuoteBefore(String ticker, LocalDateTime time) {
    Optional<Quote> optQuote = getLastQuoteBefore(ticker, time.toLocalDate(), time.toLocalTime());
    if (optQuote.isPresent()) {
      return optQuote;
    } else if (!ranges.contains(ticker, time)) {
      LocalDateTime from = time.minusMinutes(5);
      ranges.insert(ticker, from, time);
      List<Quote> quotes = fetchFromActiveTick(ticker, from, time);
      if (quotes.isEmpty()) {
        logger.error("fetched {} at {} but result is an empty list", ticker, time);
        return Optional.empty();
      }
      StringBuilder strBuilder = new StringBuilder("insert or ignore into quotes values ");
      Quote ret = null;
      int i = 0;
      for (Quote quote : Lists.reverse(quotes)) {
        if (i == 0) ret = quote;
        if (i != 0) strBuilder.append(',');
        String values =
            String.format(
                "('%s',strftime('%%s','%s'),%f,%d,%f,%d)",
                quote.ticker,
                quote.time,
                quote.askPrice,
                quote.askSize,
                quote.bidPrice,
                quote.bidSize);
        strBuilder.append(values);
        i++;
      }
      sqlite.executeUpdate(strBuilder.toString());
      if (ret == null) {
        logger.error("fetched {} at {}. but requested time is not found in result.", ticker, time);
        return Optional.empty();
      } else {
        return Optional.of(ret);
      }
    } else {
      logger.error("{} at {} is in available range but no data in db", ticker, time);
      return Optional.empty();
    }
  }
}
