package backtest.at;

import backtest.io.SQLite;
import backtest.quant.Stocks;
import backtest.utils.Cal;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.Optional;

public class Trade {
  private static final Logger logger = LoggerFactory.getLogger(Trade.class);

  public LocalDateTime time;
  public String ticker;
  public double price;
  long size;

  @Override
  public String toString() {
    return String.format("%s %s %f %d", time, ticker, price, size);
  }

  static void createSQLiteTableIfNotExist(SQLite sqlite) {
    List<String> sqls =
        ImmutableList.of(
            "create table if not exists trades ("
                + "ticker text not null, time int not null, price double not null, size int not null)",
            "create unique index if not exists index_ticker_time on trades (ticker, time)");
    sqlite.executeUpdate(sqls);
  }

  public static Optional<Trade> get(SQLite sqlite, String ticker, LocalDateTime time) {
    String sql =
        String.format(
            "select price as price, size as size from trades where ticker='%s' and time=strftime('%%s','%s')",
            ticker,
            time);
    try (ResultSet resultSet = sqlite.get(sql)) {
      if (resultSet.next()) {
        Trade trade = new Trade();
        trade.time = time;
        trade.ticker = ticker;
        trade.price = resultSet.getDouble("price");
        trade.size = resultSet.getInt("size");
        return Optional.of(trade);
      } else {
//        logger.info("result does not exist in sqlite. sql: {}", sql);
      }
    } catch (SQLException e) {
      logger.error("err in Trade::get: {}", e.getMessage());
    }
    return Optional.empty();
  }

  static Optional<Trade> firstTradeAfter(SQLite sqlite, String ticker, LocalDate date, LocalTime from) {
    String sql =
        String.format(
            "select strftime('%%Y-%%m-%%dT%%H:%%M:%%S', time,'unixepoch') as time, price as price, size as size from trades where ticker='%s' and time>=strftime('%%s','%s') and time<strftime('%%s','%s') order by time asc limit 1",
            ticker,
            LocalDateTime.of(date, from),
            LocalDateTime.of(date, LocalTime.of(15,50)));
    try (ResultSet resultSet = sqlite.get(sql)) {
      if (resultSet.next()) {
        Trade trade = new Trade();
        trade.time = LocalDateTime.parse(resultSet.getString("time"));
        trade.ticker = ticker;
        trade.price = resultSet.getDouble("price");
        trade.size = resultSet.getInt("size");
        return Optional.of(trade);
      } else {
        logger.info("result does not exist in sqlite. sql: {}", sql);
      }
    } catch (SQLException e) {
      logger.error("err in Trade::get: {}", e.getMessage());
    }
    return Optional.empty();
  }

  static Optional<Trade> firstTradeBefore(SQLite sqlite, String ticker, LocalDate date, LocalTime to) {
    String sql =
        String.format(
            "select strftime('%%Y-%%m-%%dT%%H:%%M:%%S', time,'unixepoch') as time, price as price, size as size from trades where ticker='%s' and time>=strftime('%%s','%s') and time<=strftime('%%s','%s') order by time asc limit 1",
            ticker,
            LocalDateTime.of(date, LocalTime.of(9,30)),
            LocalDateTime.of(date, to));
    try (ResultSet resultSet = sqlite.get(sql)) {
      if (resultSet.next()) {
        Trade trade = new Trade();
        trade.time = LocalDateTime.parse(resultSet.getString("time"));
        trade.ticker = ticker;
        trade.price = resultSet.getDouble("price");
        trade.size = resultSet.getInt("size");
        return Optional.of(trade);
      } else {
//        logger.info("result does not exist in sqlite. sql: {}", sql);
      }
    } catch (SQLException e) {
      logger.error("err in Trade::get: {}", e.getMessage());
    }
    return Optional.empty();
  }

  private static List<Trade> fetchFromActiveTick(
      ActiveTick at, String ticker, LocalDateTime from, LocalDateTime to) {
    final int maxRetry = 3;
    int i = 0;
    boolean success;
    int deadlineMs = 3000; // in milliseconds
    do {
      TickHistory tickHistory = new TickHistory(at, ticker, from, to);
      success = tickHistory.fetchTrade(deadlineMs * (i+1));
      if (i>0) logger.error("trial number: {}", i);
      if (success && !tickHistory.getTrade().isEmpty()) {
        return tickHistory.getTrade();
      }
      i++;
    } while (i < maxRetry);
    return ImmutableList.of();
  }

  public static Optional<Trade> getOrFetch(
      SQLite sqlite, ActiveTick at, String ticker, LocalDateTime time, TimeRanges ranges) {
    Optional<Trade> optTrade = get(sqlite, ticker, time);
    if (optTrade.isPresent()) {
      return optTrade;
    } else if (!ranges.contains(ticker, time)) {
      LocalDateTime to = time.plusMinutes(5);
      ranges.insert(ticker, time, to);
      List<Trade> trades = fetchFromActiveTick(at, ticker, time, to);
      if (trades.isEmpty()) {
        logger.error("fetched {} at {} but result is an empty list", ticker, time);
        return Optional.empty();
      }
      StringBuilder strBuilder = new StringBuilder("insert or ignore into trades values ");
      Trade ret = null;
      int i = 0;
      for (Trade trade : trades) {
        if (trade.time.isEqual(time)) ret = trade;
        if (i != 0) strBuilder.append(',');
        String values =
            String.format(
                "('%s',strftime('%%s','%s'),%f,%d)",
                trade.ticker,
                trade.time,
                trade.price,
                trade.size);
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

  public static void main(String[] args) {
    try (SQLite sqlite = new SQLite("trades.db");
         ActiveTick activeTick = new ActiveTick()) {
      Trade.createSQLiteTableIfNotExist(sqlite);
      LocalDate from = Cal.getLatestBusinessDayAfter(LocalDate.of(2016, 1, 1));
      LocalDate to = LocalDate.of(2016, 8, 13);
      List<String> sp500 = Stocks.getSP500Online();

      Optional<Trade> trade = Trade.firstTradeBefore(sqlite, "PXD", LocalDate.of(2016,8,23), LocalTime.of(10,0));
      if (trade.isPresent()) {
        System.out.println(trade.get().price);
        System.out.println(trade.get().time);
      } else {
        System.out.println("no trade");
      }

      // find ticker that doesn't have any trade in 30-35 and retry downloading from activetick.
//      for (LocalDate date = from; date.isBefore(to); date = Cal.getNextBusinessDay(date)) {
//        for (String ticker : sp500) {
//          if (!Trade.firstTradeBefore(sqlite, ticker, date, LocalTime.of(9,40)).isPresent()) {
//            LocalDateTime start = LocalDateTime.of(date, LocalTime.of(9,30));
//            LocalDateTime end = start.plusMinutes(5);
//            List<Trade> trades = fetchFromActiveTick(activeTick, ticker, start, end);
//            if (trades.isEmpty()) {
//              logger.error("fetched {} but result is an empty list", ticker);
//              continue;
//            }
//            StringBuilder strBuilder = new StringBuilder("insert or ignore into trades values ");
//            int i = 0;
//            for (Trade trade : trades) {
//              if (i != 0) strBuilder.append(',');
//              String values =
//                  String.format(
//                      "('%s',strftime('%%s','%s'),%f,%d)",
//                      trade.ticker,
//                      trade.time,
//                      trade.price,
//                      trade.size);
//              strBuilder.append(values);
//              i++;
//            }
//            sqlite.executeUpdate(strBuilder.toString());
//          }
//        }
//      }
    } catch (ConnectionException e) {
      logger.error("connection failed");
    }
  }
}
