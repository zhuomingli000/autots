package backtest.quant;

import backtest.utils.Cal;

import java.time.LocalDate;

public class SecondGap {
  public static void main(String[] args) {
    LocalDate from = LocalDate.of(2015,5,21);
    LocalDate to = LocalDate.of(2016,5,21);
    for (LocalDate date = from; date.isBefore(to); date = Cal.getNextBusinessDay(date)) {

    }
  }
}
