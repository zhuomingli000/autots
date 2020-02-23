package backtest.at;

import backtest.io.SQLite;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

class TimeRanges {
  private static final Logger logger = LoggerFactory.getLogger(TimeRanges.class);
  private final Map<String, RangeSet<LocalDateTime>> map;
  private final SQLite sqlite;

  private static void createSQLiteTableIfNotExist(SQLite sqlite) {
    String sql =
        "create table if not exists ranges (ticker text not null, from_time int not null, to_time int not null)";
    sqlite.executeUpdate(sql);
  }

  public TimeRanges(SQLite sqlite) {
    this.sqlite = sqlite;
    map = new HashMap<>();
    createSQLiteTableIfNotExist(sqlite);
    String sql =
        "select ticker as ticker, strftime('%Y-%m-%dT%H:%M:%S',from_time,'unixepoch') as from_time, strftime('%Y-%m-%dT%H:%M:%S',to_time,'unixepoch') as to_time from ranges";
    try (ResultSet resultSet = sqlite.get(sql)) {
      while (resultSet.next()) {
        String ticker = resultSet.getString("ticker");
        LocalDateTime from = LocalDateTime.parse(resultSet.getString("from_time"));
        LocalDateTime to = LocalDateTime.parse(resultSet.getString("to_time"));
        insertToMap(ticker, from, to);
      }
    } catch (SQLException e) {
      logger.error("err in time ranges fetch: {}", e.getMessage());
    }
  }

  boolean contains(String ticker, LocalDateTime time) {
    return map.containsKey(ticker) && map.get(ticker).contains(time);
  }

  private void insertToMap(String ticker, LocalDateTime from, LocalDateTime to) {
    RangeSet<LocalDateTime> set = map.getOrDefault(ticker, TreeRangeSet.create());
    set.add(Range.closedOpen(from, to));
    map.put(ticker, set);
  }

  void insert(String ticker, LocalDateTime from, LocalDateTime to) {
    insertToMap(ticker, from, to);
    String sql =
        String.format(
            "insert into ranges values ('%s',strftime('%%s','%s'),strftime('%%s','%s'))",
            ticker,
            from,
            to);
    sqlite.executeUpdate(sql);
  }

  void clear() {
    String sql = "drop table ranges";
    sqlite.executeUpdate(sql);
  }

  public static void main(String[] args) {
    try (SQLite sqlite = new SQLite("trades.db")) {
      String sql = "select ticker as ticker, strftime('%Y-%m-%dT%H:%M:%S',from_time,'unixepoch') as from_time, strftime('%Y-%m-%dT%H:%M:%S',to_time,'unixepoch') as to_time from ranges";
      try (ResultSet resultSet = sqlite.get(sql)) {
        while (resultSet.next()) {
          System.out.println(resultSet.getString("ticker") + " "  + resultSet.getString("from_time") + " " + resultSet.getString("to_time"));
        }
      } catch (SQLException e) {
        logger.error("err in time ranges fetch: {}", e.getMessage());
      }
//      TimeRanges ranges = new TimeRanges(sqlite);
//      ranges.clear();
    }
  }
}
