package backtest.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtest.quant.Stock;
import backtest.quant.Stocks;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class Util {
  private final static Logger logger = LoggerFactory.getLogger(Util.class);
  private final static double defaultEpsilon = 1e-5;

  public static boolean onlyLetter(String str) {
    for (int i = 0; i < str.length(); i++) {
      if (!isLetter(str.charAt(i))) return false;
    }
    return true;
  }

  public static boolean isLetter(char c) {
    return (c >= 65 && c <= 90) || (c >= 97 && c <= 122);
  }

  public static void serialize(Object obj, String path) {
    try {
      ObjectOutputStream oos = new ObjectOutputStream(new GZIPOutputStream(new FileOutputStream(path)));
      oos.writeObject(obj);
      oos.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static Object deserialize(String path) {
    Object data = null;
    try {
      ObjectInputStream ois = new ObjectInputStream(new GZIPInputStream(new FileInputStream(path)));
      data = ois.readObject();
      ois.close();
    } catch (IOException | ClassNotFoundException e) {
      e.printStackTrace();
    }
    return data;
  }

  /**
   * convert [start, end] to array. 0-based.
   */
  public static double[] coll2Arr(Collection<Double> coll, int start, int end) {
    if (end < start) return new double[0];
    double[] ret = new double[end - start + 1];
    int i = 0;
    for (double num : coll) {
      if (i >= start && i <= end)
        ret[i - start] = num;
      i++;
    }
    return ret;
  }

  public static double[] coll2Arr(Collection<Double> coll, int start) {
    return coll2Arr(coll, start, coll.size() - 1);
  }

  public static void run(String root, String... command) {
    ProcessBuilder processBuilder = new ProcessBuilder(command);
    processBuilder.directory(new File(root));
    try {
      Process process = processBuilder.start();
      InputStream is = process.getInputStream();
      InputStreamReader isr = new InputStreamReader(is);
      BufferedReader br = new BufferedReader(isr);
      String line;
      System.out.printf("Output of running %s is:", Arrays.toString(command));
      while ((line = br.readLine()) != null) {
        System.out.println(line);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void dumpThreads() {
    Map<Thread, StackTraceElement[]> allThreads = Thread.getAllStackTraces();
    for (Map.Entry<Thread, StackTraceElement[]> entry : allThreads.entrySet()) {
      logger.info(">>>>>>>>>>thread: {}", entry.getKey());
      for (StackTraceElement ele : entry.getValue()) {
        logger.info("stack trace element: {}", ele);
      }
    }
  }

  public static String map2str(Map map) {
    if (map == null) return "null";
    String ret = "";
    for (Object o : map.entrySet()) {
      Map.Entry entry = (Map.Entry) o;
      ret += (entry.getKey() + ": " + entry.getValue() + "\n");
    }
    return ret;
  }

  public static String list2sqlStr(List<String> list) {
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (String ticker : list) {
      if (!first) sb.append(',');
      first = false;
      sb.append('\'');
      sb.append(ticker);
      sb.append('\'');
    }
    return sb.toString();
  }

  public static String arr2Str(double[] arr) {
    StringBuilder sb = new StringBuilder();
    sb.append('[');
    boolean first = true;
    for (double n : arr) {
      if (!first) sb.append(',');
      first = false;
      sb.append(n);
    }
    sb.append(']');
    return sb.toString();
  }

  public static String arr2Str(int[] arr) {
    StringBuilder sb = new StringBuilder();
    sb.append('[');
    boolean first = true;
    for (int n : arr) {
      if (!first) sb.append(',');
      first = false;
      sb.append(String.valueOf(n));
    }
    sb.append(']');
    return sb.toString();
  }

  public static String mat2Str(double[][] mat) {
    StringBuilder sb = new StringBuilder();
    sb.append('[');
    for (int i = 0; i < mat.length; i++) {
      if (i != 0) sb.append(' ');
      sb.append(arr2Str(mat[i]));
      if (i != mat.length - 1) {
        sb.append(',');
        sb.append('\n');
      }
    }
    sb.append(']');
    return sb.toString();
  }

  /**
   * @return true if a != b or false if a == b.
   */
  public static boolean distinctDoubles(double a, double b) {
    return distinctDoubles(a, b, defaultEpsilon);
  }

  /**
   * @return true if a != b or false if a == b.
   */
  public static boolean distinctDoubles(double a, double b, double epsilon) {
    return Math.abs(a - b) > epsilon;
  }

  public static int compareDouble(double a, double b, double epsilon) {
    if (!distinctDoubles(a, b, epsilon)) return 0;
    return a < b ? -1 : 1;
  }

  /**
   * Assumes arr is increasing order.
   *
   * @return index of first element that is >= val
   */
  public static <T extends Comparable<? super T>> int indexOfFirstEleNoLessThan(ArrayList<T> arr, T val) {
    int from = 0, to = arr.size() - 1;
    while (from >= 0 && from < to) {
      int mid = from + (to - from) / 2;
      if (arr.get(mid).compareTo(val) < 0) from = mid + 1;
      else to = mid;
    }
    if (from == to && arr.get(from).compareTo(val) >= 0) return from;
    return -1;
  }

  public static <T> void printColl(Collection<T> coll) {
    for (T obj : coll) {
      System.out.println(obj);
    }
  }

  /**
   * Assumes arr is increasing order.
   *
   * @return index of first element that is <= val
   */
  public static <T extends Comparable<? super T>> int indexOfLastEleNoLargerThan(ArrayList<T> arr, T val) {
    int from = 0, to = arr.size() - 1;
    while (from >= 0 && from < to) {
      int mid = from + (to - from + 1) / 2;
      if (arr.get(mid).compareTo(val) > 0) to = mid - 1;
      else from = mid;
    }
    if (from == to && arr.get(from).compareTo(val) <= 0) return from;
    return -1;
  }
  
  public static HashSet<String> readProcessedPull(String file) throws FileNotFoundException, IOException {
    HashSet<String> result = new HashSet<String>();
    try (BufferedReader br = new BufferedReader(new FileReader(file))) {
      String line;
      while ((line = br.readLine()) != null) {
        String[] tokens = line.split("\\s+");
        result.add(line);
      }
  }
    return result;
  }
  
  public static boolean writeProcessedPull(String file, String symbol, String dateString) throws IOException {
    Files.write(Paths.get(file), String.format("%s %s\n", symbol, dateString).getBytes(), StandardOpenOption.APPEND);
    return true;
  }
  
  public static void scanProcessedFilesAndRecord(Stocks stocks, String historyLocation, String dataPrefix) throws FileNotFoundException, IOException {
    HashSet<String> spy500 = new HashSet<>(stocks.getSP500Online());
    final File folder = new File(dataPrefix);
    HashSet<String> written = new HashSet<String>();
    List<String> files = listFilesForFolder(folder);
    File processedFileLocation = new File(historyLocation);
    if (processedFileLocation.exists()) {
      processedFileLocation.delete();
    }
    processedFileLocation.createNewFile();
    for (String fileString : files) {
      BufferedReader br = new BufferedReader(new FileReader(dataPrefix + "/" + fileString));
      String line;
      while ((line = br.readLine()) != null) {
        String[] tokens = line.split("\\s+");
        if (!isValidResult(tokens, spy500))
          continue;
        String tokenComb = tokens[0] + " " + tokens[1];
        if (written.contains(tokenComb)) continue;
        written.add(tokenComb);
        System.out.println("writing " + tokenComb);
        writeProcessedPull(historyLocation, tokens[0], tokens[1]);
      }
    }
  }
  
  private static List<String> listFilesForFolder(final File folder) {
    ArrayList<String> result = new ArrayList<>();
      for (final File fileEntry : folder.listFiles()) {
          if (fileEntry.isDirectory()) {
              //listFilesForFolder(fileEntry); //not valid here
          } else {
            if (fileEntry.getName().startsWith(".")) continue;
            System.out.println("adding " + fileEntry.getName());
              result.add(fileEntry.getName());
          }
      }
      return result;
  }

  private static boolean isValidResult(String[] tokens, HashSet<String> spy500) {
    if (tokens.length < 8) return false;
    if (!spy500.contains(tokens[0])) {
      return false;
    }
    return true;
  }

  public static void sleepTo(int hour, int min) {
    sleepTo(hour, min, 0);
  }

  public static void sleepTo(int hour, int min, int second) {
    System.out.println("sleepTo called with hour: " + hour + " min: " + min + " sec: " + second);
    Duration d = durationToNext(hour, min, second);
    System.out.println("sleep duration: " + d);
    System.out.println("sleep millis: " + d.toMillis());
    try {
      Thread.sleep(d.toMillis());
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    ZoneId easternTime = ZoneId.of("America/New_York");
    System.out.println("woke up at: " + ZonedDateTime.now().withZoneSameInstant(easternTime));
  }

  private static Duration durationToNext(int hour, int min, int second) {
    ZoneId easternTime = ZoneId.of("America/New_York");
    ZonedDateTime now = ZonedDateTime.now().withZoneSameInstant(easternTime);
    ZonedDateTime to = now.withHour(hour).withMinute(min).withSecond(second);
    while (!now.isBefore(to)) {
      to = to.with(Cal.getNextBusinessDay(to.toLocalDate()));
    }
    return Duration.between(now, to);
  }

  public static void printAtSameLine(String str) {
    System.out.print("\33[2K\r" + str);
  }

  /**
   * @return current time in EST.
   */
  public static ZonedDateTime getEstNow() {
    return ZonedDateTime.now().withZoneSameInstant(ZoneId.of("America/New_York"));
  }

  public static byte[] str2Byte(String str) {
    try {
      return str.getBytes("UTF8");
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }
    return null;
  }
}
