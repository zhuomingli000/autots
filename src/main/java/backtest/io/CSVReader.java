package backtest.io;

import backtest.struct.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.function.BiConsumer;

/**
 * String should be wrapped by double quote if it contains comma.
 * if the string contains double quotes, every double quote in the string should be escaped
 * by an additional double quote.
 */
public class CSVReader {
  private Logger logger = LoggerFactory.getLogger(CSVReader.class);

  public Table readTableFromUrl(String url) {
    Table ret = new Table();
    //retry up to 3 times if error occurs.
    for (int i = 0; i < 3 && ret.isEmpty(); i++) {
      try {
        URLConnection con = new URL(url).openConnection();
        con.setConnectTimeout(20000);
        con.setReadTimeout(20000);
        Scanner sc = new Scanner(con.getInputStream());
        ret = read(sc);
      } catch (MalformedURLException e) {
        logger.error("malformed url: {}", url);
      } catch (FileNotFoundException e) {
        logger.error("404 error: {}", url);
      } catch (SocketTimeoutException e) {
        logger.error("{}: {}", e.getMessage(), url);
      } catch (IOException e) {
        logger.error("IO exception {}: {}", e.getMessage(), url);
      }
    }
    return ret;
  }

  private Table read(Scanner sc) {
    if (!sc.hasNextLine()) {
      logger.info("empty file. no header. no data. will return null");
      return new Table();
    }
    ArrayList<String> headers = line2arr(sc.nextLine());
    if (!sc.hasNextLine()) {
      logger.info("has header but no data. return null");
      return new Table();
    }
    Table ret = new Table(headers);
    while (sc.hasNextLine()) {
      String line = sc.nextLine();
      ArrayList<String> values = line2arr(line);
      for (int i = 0; i < values.size(); i++) {
        if (values.get(i).equals("")) {
          if (i == values.size() - 1 && headers.get(headers.size() - 1).equals("")) continue;
          logger.error("empty value");
          return new Table();
        }
      }
      if (values.size() != headers.size()) {
        logger.error("inconsistent size: {}", line);
        return new Table();
      } else {
        ret.append(values);
      }
    }
    return ret;
  }

  protected ArrayList<String> line2arr(String line) {
    int start = 0, end = 0;
    ArrayList<String> ret = new ArrayList<>();
    boolean escaped = false;
    boolean endQuote = false;
    while (end < line.length() && start < line.length()) {
      if (line.charAt(start) == '"') {
        escaped = true;
      }
      if (!escaped) {
        while (line.charAt(end) != ',' && end != line.length() - 1) end++;
        if (line.charAt(end) == ',') {
          ret.add(line.substring(start, end));
        } else {
          ret.add(line.substring(start));
        }
      } else {
        end++;
        if (end < line.length() && line.charAt(end) == '"') endQuote = true;
        while (end < line.length() - 1 && !(endQuote && line.charAt(end) == ',')) {
          end++;
          if (line.charAt(end) == '"') endQuote = !endQuote;
          if (line.charAt(end) != '"' && line.charAt(end) != ',') endQuote = false;
        }
        if (endQuote && end < line.length() && line.charAt(end) == ',') {
          ret.add(unescape(line.substring(start + 1, end - 1)));
        } else {
          ret.add(unescape(line.substring(start + 1, end)));
        }
      }
      if (!escaped && end == line.length() - 1 && line.charAt(end) == ',') ret.add("");
      else if (escaped && end == line.length() - 1 && endQuote && line.charAt(end) == ',') ret.add("");
      start = end + 1;
      end = start;
      escaped = false;
      endQuote = false;
    }
    return ret;
  }

  private String unescape(String str) {
    for (int i = 0; i < str.length() - 1; i++) {
      if (str.charAt(i) == '"' && str.charAt(i + 1) == '"') str = str.substring(0, i) + "\"" + str.substring(i + 2);
    }
    return str;
  }
}
