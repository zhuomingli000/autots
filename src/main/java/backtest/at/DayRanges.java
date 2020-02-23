package backtest.at;

import backtest.io.SQLite;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;

public class DayRanges {
  private static final Logger logger = LoggerFactory.getLogger(DayRanges.class);
  private final Map<String, RangeSet<LocalDate>> map;
  private final SQLite sqlite;

  private static void createSQLiteTableIfNotExist(SQLite sqlite) {
    String sql =
        "create table if not exists ranges (ticker text not null, from_time int not null, to_time int not null)";
    sqlite.executeUpdate(sql);
  }

  public DayRanges(SQLite sqlite) {
    this.sqlite = sqlite;
    map = new HashMap<>();
    createSQLiteTableIfNotExist(sqlite);
    String sql =
        "select ticker as ticker, date(from_time) as from_time, date(to_time) as to_time from ranges";
    try (ResultSet resultSet = sqlite.get(sql)) {
      while (resultSet.next()) {
        String ticker = resultSet.getString("ticker");
        LocalDate from = LocalDate.parse(resultSet.getString("from_time"));
        LocalDate to = LocalDate.parse(resultSet.getString("to_time"));
        insertToMap(ticker, from, to);
      }
    } catch (SQLException e) {
      logger.error("err in time ranges fetch: {}", e.getMessage());
    }
  }

  boolean contains(String ticker, LocalDate time) {
    return map.containsKey(ticker) && map.get(ticker).contains(time);
  }

  private void insertToMap(String ticker, LocalDate from, LocalDate to) {
    RangeSet<LocalDate> set = map.getOrDefault(ticker, TreeRangeSet.create());
    set.add(Range.closedOpen(from, to));
    map.put(ticker, set);
  }

  void insert(String ticker, LocalDate from, LocalDate to) {
    insertToMap(ticker, from, to);
    String sql =
        String.format(
            "insert into ranges values ('%s',julianday('%s'),julianday('%s'))",
            ticker,
            from,
            to);
    sqlite.executeUpdate(sql);
  }

  void clear() {
    String sql = "drop table ranges";
    sqlite.executeUpdate(sql);
  }
}
