package backtest.at;

import backtest.io.SQLite;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

public class BarHistory {
  private static final Logger logger = LoggerFactory.getLogger(BarHistory.class);
  private final SQLite sqlite;
  private final ActiveTick activeTick;
  private final DayRanges ranges;

  public BarHistory(SQLite sqlite, ActiveTick activeTick, DayRanges ranges) {
    this.sqlite = sqlite;
    this.activeTick = activeTick;
    this.ranges = ranges;
    createTableIfNotExist();
  }

  private void createTableIfNotExist() {
    String createTable = "create table if not exists ohlc (ticker text not null, date int not null, open double not null, high double not null, low double not null, close double not null, volume integer not null)";
    String createIndex = "create unique index if not exists index_ticker_date on ohlc (ticker, date)";
    sqlite.executeUpdate(ImmutableList.of(createTable, createIndex));
  }

  private Optional<OHLC> get(String ticker, LocalDate date) {
    String sql = String.format("select open as open, high as high, low as low, close as close, volume as volume from ohlc where ticker='%s' and date=julianday('%s')", ticker, date);
    try (ResultSet rs = sqlite.get(sql)) {
      if (rs.next()) {
        OHLC ohlc = new OHLC();
        ohlc.open = rs.getDouble("open");
        ohlc.high = rs.getDouble("high");
        ohlc.low = rs.getDouble("low");
        ohlc.close = rs.getDouble("close");
        ohlc.volume = rs.getLong("volume");
        ohlc.ticker = ticker;
        ohlc.date = date;
        return Optional.of(ohlc);
      } else {
//        logger.info("{} at {} does not have ohlc in sqlite", ticker, date);
      }
    } catch (SQLException e) {
      logger.error("sql exception: {} for {} at {}", e.getMessage(), ticker, date);
    }
    return Optional.empty();
  }

  private List<OHLC> fetch(String ticker, LocalDate from, LocalDate to) {
    final int maxRetry = 3;
    int i = 0;
    boolean success;
    do {
      BarGetter getter = new BarGetter(activeTick, ticker, from, to);
      success = getter.fetch();
      if (i > 0) logger.error("trial number: {}", i);
      if (success && !getter.get().isEmpty()) {
        return getter.get();
      }
      i++;
    } while (i < maxRetry);
    return ImmutableList.of();
  }

  public Optional<OHLC> getOrFetch(String ticker, LocalDate date, LocalDate fetchTo) {
    Optional<OHLC> optOhlc = get(ticker, date);
    if (optOhlc.isPresent()) {
      return optOhlc;
    } else if (!ranges.contains(ticker, date)) {
      List<OHLC> result = fetch(ticker, date, fetchTo);
      if (result.isEmpty()) {
        logger.error("fetched {} at {} but result is an empty list", ticker, date);
        return Optional.empty();
      }
      OHLC ret = null;
      StringBuilder strBuilder = new StringBuilder("insert or ignore into ohlc values ");
      int i = 0;
      for (OHLC ohlc : result) {
        if (ohlc.date.isEqual(date)) {
          ret = ohlc;
        }
        if (i != 0) strBuilder.append(',');
        String values = String.format("('%s', julianday('%s'), %f, %f, %f, %f, %d)", ticker, ohlc.date, ohlc.open, ohlc.high, ohlc.low, ohlc.close, ohlc.volume);
        strBuilder.append(values);
        i++;
      }
      sqlite.executeUpdate(strBuilder.toString());
      ranges.insert(ticker, date, fetchTo);
      if (ret == null) {
        logger.error("fetched {} at {}. but requested time is not found in result.", ticker, date);
        return Optional.empty();
      } else {
        return Optional.of(ret);
      }
    } else {
      logger.error("there is no data about {} at {} in both local and activetick", ticker, date);
      return Optional.empty();
    }
  }

  public static void main(String[] args) {
    try (SQLite sqlite = new SQLite("test.db");
         ActiveTick activeTick = new ActiveTick()) {
      DayRanges ranges = new DayRanges(sqlite);
      BarHistory getter = new BarHistory(sqlite, activeTick, ranges);
      getter.createTableIfNotExist();
      Optional<OHLC> optOhlc = getter.getOrFetch("SRCL", LocalDate.of(2016, 8, 8), LocalDate.of(2016,9,1));
      if (optOhlc.isPresent()) {
        System.out.println(optOhlc.get());
      } else {
        System.out.println("not found");
      }
    } catch (ConnectionException e) {
      logger.info("connection fails");
    }
  }
}
