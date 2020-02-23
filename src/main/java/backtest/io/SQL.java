package backtest.io;

import backtest.utils.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class SQL {
  private final static Logger logger = LoggerFactory.getLogger(SQL.class);

  /**
   * isolation level is set to read commited (no access in transaction)
   */
  public static Connection getConnection(String database) {
    Connection con;
    String username = Config.getInstance().get("username");
    String password = Config.getInstance().get("password");
    String host = Config.getInstance().get("host");
    try {
      logger.info("create new connection");
      con = DriverManager.getConnection("jdbc:mysql://" + host + ":3306/" + database +
          "?rewriteBatchedStatements=true&autoReconnect=true&failOverReadOnly=false" +
          "&maxReconnects=10", username, password);
      con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
      return con;
    } catch (SQLException e) {
      logger.error("fail to get connection");
      printError(e);
    }
    return null;
  }

  public static void closeConnection(Connection con) {
    try {
      if (con != null) {
        con.close();
      }
      logger.info("connection closed.");
    } catch (SQLException e) {
      logger.error("fail to close connection");
      printError(e);
    }
  }

  public static List<String> getStrColInTable(Connection con, String table, String field) {
    return getStrCol(con, "SELECT " + field + " FROM " + table, field);
  }

  public static List<String> getStrCol(Connection con, String statement, String field) {
    List<String> res = new ArrayList<>();
    try (Statement st = con.createStatement();
         ResultSet rs = st.executeQuery(statement)) {
      while (rs.next()) {
        res.add(rs.getString(field));
      }
    } catch (SQLException e) {
      logger.error("error in getStrCol. statement: {}. result field: {}", statement, field);
      printError(e);
    }
    return res;
  }

  public static int getInt(Connection con, String statement, String field) {
    try (Statement st = con.createStatement();
         ResultSet rs = st.executeQuery(statement)) {
      if (rs.next()) {
        return rs.getInt(field);
      }
    } catch (SQLException e) {
      logger.error("error in getInt, statement: {}, field: {}.", statement, field);
      printError(e);
    }
    logger.error("no result found in getInt, statement, {}, field: {}", statement, field);
    return -1; //I think -1 is the easiest number to cause some exceptions (index out of bound).
  }

  public static double getDouble(Connection con, String statement, String field) {
    try (Statement st = con.createStatement();
         ResultSet rs = st.executeQuery(statement)) {
      if (rs.next()) {
        return rs.getDouble(field);
      }
    } catch (SQLException e) {
      logger.error("error in getDouble, statement: {}, field: {}", statement, field);
      printError(e);
    }
    logger.error("no result found in getDouble, statement, {}, field: {}", statement, field);
    return Double.NaN;
  }

  public static void executeUpdate(Connection con, String statement) {
    try (Statement st = con.createStatement()) {
      st.executeUpdate(statement);
    } catch (SQLException e) {
      logger.error("error in executeUpdate: {}", e.getMessage());
//            logger.debug("update statement: {}", statement);
      printError(e);
    }
  }

  public static void printError(SQLException e) {
    logger.error(e.getMessage());
    e.printStackTrace();
  }

  public static void clearTable(Connection con, String table) {
    executeUpdate(con, "DELETE FROM " + table);
  }

  public static void savePreparedStatement(PreparedStatement pst, String path) {
    try (PrintWriter pw = new PrintWriter(path)) {
      pw.println(pst);
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
  }

  public static boolean hasResult(Connection con, String statement) {
    try (Statement st = con.createStatement();
         ResultSet rs = st.executeQuery(statement)) {
      return rs.isBeforeFirst();
    } catch (SQLException e) {
      logger.error("error in hasResult, statement: {}, field: {}", statement);
      printError(e);
    }
    return false;
  }

  public static void resetAutoIncrement(Connection con, String table) {
    executeUpdate(con, "ALTER TABLE " + table + " AUTO_INCREMENT = 1");
  }
}
