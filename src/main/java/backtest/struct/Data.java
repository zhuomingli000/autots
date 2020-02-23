package backtest.struct;

public class Data {
  String symbol;
  public String getSymbol() {
    return symbol;
  }

  public void setSymbol(String symbol) {
    this.symbol = symbol;
  }

  public String getDateTime() {
    return dateTime;
  }

  public void setDateTime(String dateTime) {
    this.dateTime = dateTime;
  }

  public double getOpen() {
    return open;
  }

  public void setOpen(double open) {
    this.open = open;
  }

  public double getClose() {
    return close;
  }

  public void setClose(double close) {
    this.close = close;
  }

  public double getHigh() {
    return high;
  }

  public void setHigh(double high) {
    this.high = high;
  }

  public double getLow() {
    return low;
  }

  public void setLow(double low) {
    this.low = low;
  }

  public int getVolumn() {
    return volumn;
  }

  public Data setVolumn(int volumn) {
    this.volumn = volumn;
    return this;
  }

  String dateTime;
  double open;
  double close;
  double high;
  double low;
  int volumn;
  double ask;
  double bid;

 public Data setAsk(double a) {
   ask = a;
   return this;
 }
 
 public double getAsk() {
   return ask;
 }

 public Data setBid(double b) {
   bid = b;
   return this;
 }
 
 public double getBid() {
   return bid;
 }
 
  public Data (String sym, String time, String open, String high, String low, String close, String vol) {
    symbol = sym;
    dateTime = time;
    this.open = Double.valueOf(open);
    this.close = Double.valueOf(close);
    this.high = Double.valueOf(high);
    this.low = Double.valueOf(low);
    this.volumn = Integer.valueOf(vol);
  }
  
  public Data (String sym, String time) {
    symbol = sym;
    dateTime = time;
  }
}