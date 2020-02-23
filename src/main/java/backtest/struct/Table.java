package backtest.struct;

import backtest.io.CSVWriter;

import java.time.LocalDate;
import java.util.*;

public class Table {
  /**
   * map column name to column index
   */
  private Map<String, Integer> c2i;
  private ArrayList<ArrayList<String>> data;
  private int numRow;
  private int numCol;
  private String name;

  /**
   * this method is only for creating an empty table.
   */
  public Table() {
    numRow = 0;
  }

  public Table(List<String> cols) {
    numCol = cols.size();
    c2i = new HashMap<>();
    for (int i = 0; i < numCol; i++) {
      c2i.put(cols.get(i), i);
    }
    data = new ArrayList<>();
    numRow = 0;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public void append(ArrayList<String> row) {
    data.add(row);
    numRow++;
  }

  public void append(Map<String, String> row) {
    ArrayList<String> arr = new ArrayList<>();
    for (int i = 0; i < numCol; i++) arr.add("");
    for (Map.Entry<String, String> entry : row.entrySet()) {
      arr.set(c2i.get(entry.getKey()), entry.getValue());
    }
    append(arr);
  }

  public void append(String... row) {
    append(new ArrayList<>(Arrays.asList(row)));
  }

  public int getNumRow() {
    return numRow;
  }

  public int getNumCol() {
    return c2i.size();
  }

  public String get(int r, String col) {
    Integer ci = c2i.get(col);
    if (ci == null) return null;
    return data.get(r).get(ci);
  }

  public ArrayList<String> get(int r) {
    return data.get(r);
  }

  public Map<String, String> getMap(int r) {
    Map<String, String> ret = new HashMap<>();
    for (Map.Entry<String, Integer> entry : c2i.entrySet()) {
      ret.put(entry.getKey(), data.get(r).get(entry.getValue()));
    }
    return ret;
  }

  /**
   * @throws NullPointerException if col does not exist because null can't be converted to int
   */
  public int getColId(String col) {
    return c2i.get(col);
  }

  public Map<String, Integer> getHeader() {
    return c2i;
  }

  public void sort(Comparator<ArrayList<String>> comparator) {
    data.sort(comparator);
  }

  public void insert(int i, ArrayList<String> row) {
    data.add(i, row);
    numRow++;
  }

  public boolean isEmpty() {
    return getNumRow() == 0;
  }

  public ArrayList<String> getCols() {
    ArrayList<String> ret = new ArrayList<>(c2i.keySet());
    for (Map.Entry<String, Integer> entry : c2i.entrySet()) {
      ret.set(entry.getValue(), entry.getKey());
    }
    return ret;
  }

  public void toCSV(String path) {
    if (isEmpty()) return;
    CSVWriter writer = new CSVWriter(path, getCols());
    for (int i = 0; i < getNumRow(); i++) {
      writer.println(get(i));
    }
    writer.close();
  }

  public int find(LocalDate date) {
    for (int i = 0; i<getNumRow(); i++) {
      if (get(i).get(0).equals(date.toString())) {
        return i;
      }
    }
    return -1;
  }
}
