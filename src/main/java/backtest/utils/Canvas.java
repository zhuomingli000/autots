package backtest.utils;

import backtest.struct.TimeSeries;

import java.awt.*;
import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class Canvas {
  private String output;
  private Map<Integer, List<String>> data;
  private boolean showInd;

  public Canvas() {
    this("output/output.html");
  }

  public Canvas(String output) {
    this.output = output;
    data = new TreeMap<>();
    showInd = false;
  }

  public void setShowInd(boolean flag) {
    showInd = flag;
  }

  /**
   * different from addSeries(Series, int), this function draws
   * all series to the first canvas
   */
  public void addSeries(TimeSeries series) {
    addSeries(series, 0);
  }

  public void addSeries(TimeSeries series, int figureId) {
    if (!data.containsKey(figureId)) data.put(figureId, new LinkedList<>());
    data.get(figureId).add(series.toString());
  }

  private String dataToStr() {
    StringBuilder sb = new StringBuilder();
    sb.append('{');
    boolean f0 = true;
    for (Map.Entry<Integer, List<String>> entry : data.entrySet()) {
      if (!f0) sb.append(',');
      f0 = false;
      sb.append(entry.getKey());
      sb.append(":[");
      boolean f1 = true;
      for (String str : entry.getValue()) {
        if (!f1) sb.append(',');
        f1 = false;
        sb.append(str);
      }
      sb.append(']');
    }
    sb.append('}');
    return sb.toString();
  }

  private HTMLTemplate fillTemplate() {
    HTMLTemplate html = new HTMLTemplate("plot.html");
    html.replace("data", dataToStr());
    html.replace("showInd", String.valueOf(showInd));
    return html;
  }

  public void draw() {
    HTMLTemplate html = fillTemplate();
    html.save(output);
    File htmlFile = new File(output);
    try {
      Desktop.getDesktop().browse(htmlFile.toURI());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public String toString() {
    return fillTemplate().toString();
  }
}
