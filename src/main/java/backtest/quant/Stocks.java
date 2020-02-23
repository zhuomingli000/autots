package backtest.quant;

import backtest.io.CSVReader;
import backtest.io.SQL;
import backtest.io.YahooDataCmp;
import backtest.struct.Table;
import backtest.struct.TimeSeries;
import backtest.utils.*;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.time.LocalDate;
import java.time.Period;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class Stocks {
  private final static Logger logger = LoggerFactory.getLogger(Stocks.class);
  private static String database = Config.getInstance().get("database");
  private Connection con;
  private HashMap<String, LocalDate> firstDayMap;
  private HashMap<String, LocalDate> lastDayMap;
  private LoadingCache<String, Stock> cache;
  private boolean cacheDisabled;

  public Stocks() {
    con = SQL.getConnection(database);
    getAllStocks();
    cacheDisabled = true;
  }

  public void reconnect() {
    logger.info("reconnect Stocks sql connection");
    SQL.closeConnection(con);
    con = SQL.getConnection(database);
  }

  public double getAdjCloseSQL(String ticker, LocalDate day) {
    return getDouble(ticker, "adj_close_price", day);
  }

  public double getCloseSQL(String ticker, LocalDate day) {
    return getDouble(ticker, "close_price", day);
  }

  @SuppressWarnings("unused")
  public double getAdjOpenSQL(String ticker, LocalDate day) {
    return SQL.getDouble(getConnection(), "SELECT dp.open_price*dp.adj_close_price/dp" +
        ".close_price AS res FROM daily_price AS dp INNER JOIN symbol as sym ON dp" +
        ".symbol_id = sym.id WHERE sym.ticker='" + ticker + "' AND dp.price_date='" + day
        + "'", "res");
  }

  public void updateTo(List<String> list, LocalDate to) {
    List<String> toUpdate = new ArrayList<>();
    for (String ticker : list) {
      if (!has(ticker) || getLastDay(ticker).isBefore(Cal.getPrevBusinessDay(to))) toUpdate.add(ticker);
    }
    if (toUpdate.isEmpty()) {
      logger.info("All symbols requested are up to date so skipping updating.");
      return;
    }
    logger.info("will pull historical data from yahoo for {} stocks", toUpdate.size());
    if (updateData(toUpdate)) {
      String str = Util.list2sqlStr(toUpdate);
      updateDayRange(str);
      recreate();
    }
  }

  /**
   * Currently a stock is valid if
   * 1. it is in Yahoo (we only get stocks in yahoo in getAllStocks method so no need to check
   * this. check whether is in all map)
   * 2. first available timestamp is earlier than the given time
   * 3. last available timestamp is later than the given time.
   */
  public boolean isValid(String ticker, LocalDate time) {
    return has(ticker) && !(time.isBefore(getFirstDay(ticker)) || time.isAfter(getLastDay(ticker)));
  }

  /**
   * this method only checks whether symbol table has given stock
   * and whether that stock has some price data.
   */
  public boolean has(String ticker) {
    return firstDayMap.containsKey(ticker) && lastDayMap.containsKey(ticker);
  }

  public LocalDate getFirstDay(String ticker) {
    return firstDayMap.get(ticker);
  }

  public LocalDate getLastDay(String ticker) {
    return lastDayMap.get(ticker);
  }

  public void recreate() {
    getAllStocks();
    if (cache != null)
      cache.invalidateAll();
  }

  /**
   * get stock from cache. if not cached yet, will cache stock requested.
   */
  public Stock getStockFromCache(String ticker) {
    if (cache == null || cacheDisabled) return getStockFromDB(ticker);
    try {
      return cache.get(ticker);
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
    return null;
  }

  /**
   * get stock directly from database. no cache.
   */
  public Stock getStockFromDB(String ticker) {
    return new Stock(ticker, getConnection());
  }

  /**
   * @param end end is not included in the window. should be of the form of yyyy-mm-dd
   */
  @SuppressWarnings("unused")
  public double movingAvgSQL(String ticker, String end, int window, String colName) {
    return SQL.getDouble(getConnection(), "SELECT AVG(dp." + colName + ") AS res FROM " +
        "daily_price AS dp INNER JOIN symbol AS sym ON dp.symbol_id=sym.id WHERE sym" +
        ".ticker='" + ticker + "' AND dp.price_date<'" + end + "' ORDER BY dp.price_date " +
        "LIMIT " + window, "res");
  }

  public static List<String> getSP500Online() {
    return getSPListOnline("http://en.wikipedia.org/wiki/List_of_S&P_500_companies");
  }

  @SuppressWarnings("unused")
  public List<String> getSP100Online() {
    return getSPListOnline("http://en.wikipedia.org/wiki/S&P_100");
  }

  public static List<String> getSPListOnline(String url) {
    logger.info("fetch {}", url);
    List<String> list = new ArrayList<>();
    try {
      Document doc = Jsoup.connect(url)
          .userAgent("Mozilla/5.0 (Windows; U; WindowsNT 5.1; en-US; rv1.8.1.6) " +
              "Gecko/20070725 Firefox/2.0.0.6")
          .maxBodySize(16 * 1024 * 1024).timeout(5000).get();
      Element table = doc.select(".wikitable.sortable").first();
      Elements rows = table.select("tr");
      for (Element row : rows) {
        Elements cells = row.select("td");
        if (cells.size() == 0) {
          continue;
        }
        String ticker = cells.first().text();
        if (!Util.onlyLetter(ticker)) continue;
        list.add(ticker);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return list;
  }

  public List<String> getSP500AndSPY() {
    return SQL.getStrCol(getConnection(), "select ticker from symbol", "ticker");
  }

  public List<String> getSP500() {
    List<String> ret = getSP500AndSPY();
    ret.remove("SPY");
    return ret;
  }
  
  public Set<String> getSP500Set() {
    List<String> ret = getSP500AndSPY();
    ret.remove("SPY");
    return new HashSet<>(ret);
  }

  /**
   * only for single day fetch. use Stock::getExtremeDays instead for multiple days.
   */
  public List<String> get52WeekHigh(LocalDate date) {
    date = Cal.getPrevBusinessDay(date);
    // @formatter:off
    String statement =
      "SELECT sym.ticker AS ticker " +
      "FROM symbol AS sym " +
      "INNER JOIN " +
        "(SELECT high.id AS id " +
         "FROM " +
           "(SELECT symbol_id AS id, MAX(high_price) AS max_price " +
            "FROM daily_price " +
            "WHERE price_date>DATE_SUB('" + date +"', INTERVAL 1 YEAR) " +
            "GROUP BY symbol_id) AS high " +
         "INNER JOIN " +
           "(SELECT symbol_id AS id, " +
            "high_price AS last_price " +
            "FROM daily_price " +
            "WHERE price_date='" + date + "') AS last " +
         "ON high.id=last.id " +
         "WHERE high.max_price<=last.last_price) AS ids " +
      "ON sym.id=ids.id";
    // @formatter:on
    return SQL.getStrCol(getConnection(), statement, "ticker");
  }

  public void updateTo(LocalDate to, String... list) {
    updateTo(Arrays.asList(list), to);
  }

  public void updateSP500AndSPY() {
    logger.info("update all for sql");
    updateSP500AndSPYTo(LocalDate.now());
  }

  public void updateSP500AndSPYTo(LocalDate to) {
    logger.info("start updating price data of SP500");
    List<String> sp500Symbols = getSP500Online();
    sp500Symbols.add("SPY");
    delistSymbols(sp500Symbols);
    insertSymbols(sp500Symbols);
    recreate();
    updateTo(getSP500AndSPY(), to);
  }

  /**
   * get all stocks that are in Yahoo
   * only get stocks that have not null first_day and last_day
   * so this function does not get stocks that don't have any price data
   */
  private void getAllStocks() {
    logger.info("get all stock summary from db to memory");
    firstDayMap = new HashMap<>();
    lastDayMap = new HashMap<>();
    String statement = "SELECT ticker, first_day, last_day FROM symbol WHERE first_day IS NOT" +
        " NULL AND last_day IS NOT NULL";
    try (Statement st = getConnection().createStatement();
         ResultSet rs = st.executeQuery(statement)) {
      while (rs.next()) {
        String ticker = rs.getString("ticker");
        LocalDate firstDay = rs.getDate("first_day").toLocalDate();
        LocalDate lastDay = rs.getDate("last_day").toLocalDate();
        firstDayMap.put(ticker, firstDay);
        lastDayMap.put(ticker, lastDay);
      }
    } catch (SQLException e) {
      logger.error("error in getAllStocks");
      logger.error(e.getMessage());
      e.printStackTrace();
    }
  }

  /**
   * return invalid connection if reconnecting fails
   */
  private Connection getConnection() {
    try {
      if (con == null || !con.isValid(5)) reconnect();
    } catch (SQLException e) {
      reconnect();
    }
    return con;
  }

  private double getDouble(String ticker, String field, LocalDate day) {
    return SQL.getDouble(getConnection(), "SELECT dp." + field + " FROM daily_price AS dp " +
        "INNER JOIN symbol as sym ON dp.symbol_id = sym.id WHERE sym.ticker='" + ticker +
        "' AND dp.price_date='" + day + "'", field);
  }

  /**
   * 1. if does not exist. insert all rows
   * 2. if already exists, first check if adjusted price changes. if changes, update. then
   * insert new rows
   * 3. update stocks instance as well if exist. (to update first and last day in stocks instance)
   *
   * @return false if nothing is changed otherwise true
   */
  private boolean updateData(List<String> symbols) {
    logger.info("start updating data");
    //batchSize is related to max_allowed_packet. need to make allowed packet larger
    final int batchSize = 500;
//        YahooDataGetter dataGetter = new YahooDataGetter(10);
//        dataGetter.start(symbols);
    List<List<String>> insertTable = new ArrayList<>();
    List<List<String>> updateTable = new ArrayList<>();
    logger.info("update price data of {} symbols in total", symbols.size());
    boolean changed = false;
    for (int i = 0; i < symbols.size(); i++) {
      String ticker = symbols.get(i);
      String url = "http://ichart.finance.yahoo.com/table.csv?s=" + ticker;
      System.out.print("\33[2K\r" + i + " done. next: " + ticker + ". " + (int)(i/(double)symbols.size()*100) + "%");
      Table table = new CSVReader().readTableFromUrl(url);
      table.setName(ticker);
      //Note that forwardFill check whether there are too many missing data. if so, skip.
      if (table.isEmpty() || outOfRange(table) || forwardFill(table)) {
        continue;
      }
      changed = true;
      int sid = SQL.getInt(getConnection(), "SELECT id FROM symbol WHERE ticker='"
          + ticker + "'", "id");
      if (isNew(ticker)) {
        insertTableToLL(insertTable, table, 0, sid);
      } else {
        LocalDate lastDay = getLastDay(ticker);
        int lastDayRow = searchRowIndex(table, lastDay);
        if (lastDayRow == -1) {
          logger.error("lastDayRow not found: ticker: {}, sid is {}, lastDay is {}",
              ticker, sid, lastDay);
          continue;
        }
//                logger.debug("start updating adjusted price and insert new rows of {}, sid is
// {}, lastDayRow is {}", ticker, sid, lastDayRow);
        if (adjPricChange(table)) {
          updateTableToLL(updateTable, table, Arrays.asList("Date", "Adj Close"), 0,
              lastDayRow, sid);
        }
        insertTableToLL(insertTable, table, lastDayRow + 1, sid);
      }
      if (insertTable.size() > batchSize) {
        insertNewTable(insertTable);
      }
      if (updateTable.size() > batchSize) {
        insertUpdateTable(updateTable);
      }
    }
    System.out.println();
    if (!insertTable.isEmpty()) {
      insertNewTable(insertTable);
    }
    if (!updateTable.isEmpty()) {
      insertUpdateTable(updateTable);
    }
    logger.info("all symbols done");
    return changed;
  }

  /**
   * all number should fit into decimal(19,4).
   * sql will automatically round number if decimal part has more than 5 digits.
   * here we consider number as out of range if it has more than 15 digits in integer part.
   */
  private boolean outOfRange(Table table) {
    for (int i = 0; i < table.getNumRow(); i++) {
      for (int j = 0; j < table.getNumCol(); j++) {
        String txt = table.get(i).get(j);
        if (txt == null) continue;
        // table given is a mix of string and number
        // we want all numbers are in the range but don't have constraints on string
        // try block below is used to ignore all entries that are not number.
        try {
          //noinspection ResultOfMethodCallIgnored
          Double.parseDouble(txt);
          String[] arr = txt.split("\\.");
//                    if (arr[0].length() > 15 || (arr.length > 1 && arr[1].length() > 4)) {
          if (arr[0].length() > 15) {
            logger.error("{} has number out of range", table.getName());
            return true;
          }
        } catch (NumberFormatException ignored) {
        }
      }
    }
    return false;
  }

  private void insertTableToLL(List<List<String>> ll, Table table, int from, int sid) {
    for (int i = from; i < table.getNumRow(); i++) {
      List<String> newRow = table.get(i);
      newRow.add(Integer.toString(sid));
      ll.add(newRow);
    }
  }

  private void updateTableToLL(List<List<String>> ll, Table table, List<String> cols, int from,
                               int to, int sid) {
    for (int i = from; i < to; i++) {
      Map<String, String> row = table.getMap(i);
      List<String> newRow = new ArrayList<>();
      for (String col : cols) {
        newRow.add(row.get(col));
      }
      newRow.add(Integer.toString(sid));
      ll.add(newRow);
    }
  }

  private void insertNewTable(List<List<String>> ll) {
    StringBuilder statement = new StringBuilder("INSERT INTO daily_price (price_date, " +
        "open_price, high_price, low_price, close_price, volume, adj_close_price, " +
        "symbol_id) VALUES");
    batchInsert(statement, ll);
    SQL.executeUpdate(getConnection(), statement.toString());
    ll.clear();
  }

  private void insertUpdateTable(List<List<String>> ll) {
    SQL.executeUpdate(getConnection(), "CREATE TEMPORARY TABLE IF NOT EXISTS update_tmp " +
        "(`sid` int NOT NULL, `date` date NOT NULL, `adj_close_price` decimal(19,4) NOT " +
        "NULL, KEY `index_sym_id` (`sid`), KEY `index_date` (`date`))");
    SQL.executeUpdate(getConnection(), "TRUNCATE TABLE update_tmp");
    StringBuilder statement = new StringBuilder("INSERT INTO update_tmp (date, " +
        "adj_close_price, sid) VALUES");
    batchInsert(statement, ll);
    SQL.executeUpdate(getConnection(), statement.toString());
    ll.clear();
    SQL.executeUpdate(getConnection(), "UPDATE daily_price AS dp INNER JOIN update_tmp AS tmp" +
        " ON dp.symbol_id=tmp.sid AND dp.price_date=tmp.date SET dp.adj_close_price=tmp" +
        ".adj_close_price");
  }

  private void batchInsert(StringBuilder sb, List<List<String>> ll) {
    for (int i = 0; i < ll.size(); i++) {
      List<String> row = ll.get(i);
      if (row.size() < 2) continue;
      sb.append(i == 0 ? " ('" : ", ('");
      sb.append(row.get(0));//date. wrap with quotes
      sb.append("',");
      int rowSize = row.size();
      for (int j = 1; j < rowSize; j++) {
        sb.append(row.get(j));
        sb.append(j == rowSize - 1 ? ')' : ',');
      }
    }
  }

  private boolean adjPricChange(Table table) {
    String ticker = table.getName();
    LocalDate lastDay = getLastDay(ticker);
    double prev = getAdjCloseSQL(ticker, lastDay);
    Map<String, String> row = searchRow(table, lastDay);
    if (row == null) return false;
    double now = Double.parseDouble(row.get("Adj Close"));
    return Math.abs(prev - now) > 1e-6;
  }

  /**
   * check if a stock is just added into db and waiting for initial data fetching
   */
  private boolean isNew(String ticker) {
    return !has(ticker);
  }

  /**
   * forward fill gaps in csv files from yahoo finance
   *
   * @return whether there is too many missing data
   */
  private boolean forwardFill(Table table) {
    if (table == null || table.getNumRow() == 0) return true;
//        logger.info("start forwardfilling {}", table.getName());
    table.sort(new YahooDataCmp(table.getHeader()));
    int di = table.getColId("Date");
    LocalDate prev = LocalDate.parse(table.get(0).get(di));
    int cnt = 0;
    for (int i = 1; i < table.getNumRow(); i++) {
      LocalDate expected = Cal.getNextBusinessDay(prev);
      LocalDate actual = LocalDate.parse(table.get(i).get(di));
      //sometimes actual is before expected
      if (actual.isAfter(expected)) {
        ArrayList<String> newRow = new ArrayList<>(table.get(i - 1));
        newRow.set(di, expected.toString());
        table.insert(i, newRow);
        cnt++;
        actual = expected;
      }
      prev = actual;
    }
    if (cnt > 75) {
      logger.error("{} has too many missing data. please check", table.getName());
      return true;
    }
    return false;
  }

  private Map<String, String> searchRow(Table table, LocalDate day) {
    int i = searchRowIndex(table, day);
    if (i == -1) return null;
    return table.getMap(i);
  }

  private int searchRowIndex(Table table, LocalDate day) {
    int from = 0, to = table.getNumRow() - 1;
    int di = table.getColId("Date");
    while (from <= to) {
      int mid = (from + to) >>> 1;
      LocalDate midDay = LocalDate.parse(table.get(mid).get(di));
      if (midDay.isEqual(day))
        return mid;
      else if (midDay.isBefore(day))
        from = mid + 1;
      else
        to = mid - 1;
    }
    // not found
    return -1;
  }

  private void updateDayRange(String list) {
    logger.info("start updating stock summary");
    if (list.isEmpty()) return;
    SQL.executeUpdate(getConnection(), "UPDATE symbol sym INNER JOIN (select dp.symbol_id, " +
        "MIN(dp.price_date) first_day, MAX(dp.price_date) last_day from daily_price as dp" +
        " inner join symbol as sym on dp.symbol_id=sym.id where sym.ticker in (" + list +
        ") group by dp.symbol_id) dr ON sym.id=dr.symbol_id SET sym.first_day=dr" +
        ".first_day, sym.last_day=dr.last_day");
  }

  private void delistSymbols(List<String> validSymbols) {
    logger.info("check symbols that are delisted");
    List<String> preSymbols = SQL.getStrCol(getConnection(), "SELECT ticker FROM symbol", "ticker");
    Set<String> validSet = new HashSet<>(validSymbols);
    //if foreign key is removed. will also need to delete data from daily_price
    try (PreparedStatement pst = getConnection().prepareStatement("DELETE FROM symbol WHERE " +
        "ticker=?")) {
      getConnection().setAutoCommit(false);
      for (String symbol : preSymbols) {
        if (!validSet.contains(symbol)) {
          logger.info("delist {}", symbol);
          pst.setString(1, symbol);
          pst.addBatch();
        }
      }
      pst.executeBatch();
      getConnection().commit();
      getConnection().setAutoCommit(true);
    } catch (SQLException e) {
      logger.error("error when delist symbols");
      SQL.printError(e);
    }
  }

  private void insertSymbols(List<String> symbols) {
    logger.debug("inserting {} symbols", symbols.size());
    try (PreparedStatement pst = getConnection().prepareStatement("INSERT INTO symbol " +
        "(ticker) VALUES(?) ON DUPLICATE KEY UPDATE id=id")) {
//            getConnection().setAutoCommit(false);
      for (String symbol : symbols) {
        if (!Util.onlyLetter(symbol)) continue;
        pst.setString(1, symbol);
        pst.addBatch();
      }
      pst.executeBatch();
//            getConnection().commit();
//            getConnection().setAutoCommit(true);
    } catch (SQLException e) {
      logger.error("sql error when insert symbols");
      SQL.printError(e);
    }
  }

  public TimeSeries getValidStockNumSeries(List<String> tickers, LocalDate from,
                                           LocalDate to) {
    logger.info("start getting number of valid stocks");
    TimeSeries ret = new TimeSeries(from, to, tickers.size());
    for (String ticker : tickers) {
//            System.out.println("check " + i + " th ticker");
      LocalDate firstDay = getFirstDay(ticker);
      if (firstDay == null) {
        System.out.println("stop");
        return ret;
      }
      for (Map.Entry<LocalDate, Num> entry : ret.entrySet()) {
        if (entry.getKey().isBefore(firstDay)) entry.setValue(new Num(entry.getValue().toDouble() - 1));
        else break;
      }
    }
    return ret;
  }

  @SuppressWarnings("unused")
  private void TestStockGetExtremeDays() {
    updateTo(LocalDate.of(2015, 11, 1), "AAPL");
    Stock aapl = getStockFromDB("AAPL");
    for (LocalDate d : aapl.getExtremeDays(LocalDate.of(2014, 1, 1), LocalDate.of(2015, 10, 1),
        Period.ofMonths(1), false)) {
      System.out.println(d);
    }
  }

  @SuppressWarnings("unused")
  private void TestStocksGetValidStockNumSeries() {
//        insertSP500();
//        updateSP500AndSPYTo(LocalDate.of(2015, 11, 1));
    System.out.println(getValidStockNumSeries(getSP500AndSPY(), LocalDate.of(2005, 1,
        1), LocalDate.of(2015, 10, 1)));
  }

  @SuppressWarnings("unused")
  private void TestGetBearDays() {
    Stock spy = getStockFromDB("SPY");
    Util.printColl(spy.getBearDates(LocalDate.of(2000, 1, 1), LocalDate.of(2016, 1, 1), 0.15, 20));
  }

  public static void main(String[] args) {
    Stocks stocks = new Stocks();
    stocks.updateSP500AndSPY();
//        stocks.TestStockGetExtremeDays();
//        stocks.TestStocksGetValidStockNumSeries();
    //stocks.TestGetBearDays();
//    Util.printColl(stocks.get52WeekHigh(LocalDate.now()));
  }

  public void cacheAllStockQueries() {
    if (cache == null) {
      cache = CacheBuilder.newBuilder().build(new CacheLoader<String, Stock>() {
        @Override
        public Stock load(String key) throws Exception {
          return getStockFromDB(key);
        }
      });
      cacheDisabled = false;
    }
  }

  public void load(List<String> list) {
    for (int i = 0; i<list.size(); i++) {
      System.out.print("\33[2K\rloading " + list.get(i) + " from db " + (int)(i/(double)list.size()*100) + "%");
      getStockFromCache(list.get(i));
    }
    System.out.println();
  }
}