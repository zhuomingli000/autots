package backtest.io;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.List;

public class SQLite implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(SQLite.class);
  private Connection connection;
  private Statement statement;

  public SQLite(String dbFileName){
    try {
      // create a database connection
      connection = DriverManager.getConnection("jdbc:sqlite:" + dbFileName);
      statement = connection.createStatement();
      statement.setQueryTimeout(30);  // set timeout to 30 sec.
    } catch(SQLException e) {
      // if the error message is "out of memory",
      // it probably means no database file is found
      logger.error(e.getMessage());
    }
  }

  public void executeUpdate(String sql) {
    try {
      statement.executeUpdate(sql);
    } catch (SQLException e) {
      logger.error("error in executeUpdate: {}, sql: {}", e.getMessage(), sql);
    }
  }

  public void executeUpdate(List<String> sqls) {
    sqls.forEach(this::executeUpdate);
  }

  public ResultSet get(String sql) throws SQLException {
    return statement.executeQuery(sql);
  }

  public void beginTransaction() {
    try {
      connection.setAutoCommit(false);
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  public void commitTransaction() {
    try {
      connection.commit();
      connection.setAutoCommit(true);
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void close() {
    try {
      if (statement != null) statement.close();
      if (connection != null) connection.close();
    } catch(SQLException e) {
      // connection close failed.
      logger.error(e.getMessage());
    }
  }
}
