package backtest.ib;

import backtest.quant.Stocks;
import backtest.utils.Util;
import com.ib.client.Types;

import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;

public class DataGetter {
  public static void main(String[] args) throws IOException, InterruptedException {
    Stocks stocks = new Stocks();
    IBController ibController = new IBController();
    if (!ibController.connect("127.0.0.1", 7497, 1)) return;

    Util.scanProcessedFilesAndRecord(stocks, "processedPull30Min.txt", "data");
    HashSet<String> processed = Util.readProcessedPull("processedPull30Min.txt");
    LocalDate date =LocalDate.of(2015, 2, 1);
    while (date.isBefore(LocalDate.of(2016, 3, 2))) {
      for (String ticker : stocks.getSP500()) {
        if (processed.contains(String.format("%s %s", ticker, date.toString()))) {
          System.out.println("skipping " + String.format("%s %s", ticker, date.toString()));
          continue;
        }
        ZoneId easternTime = ZoneId.of("America/New_York");
        ZonedDateTime now = ZonedDateTime.now().withZoneSameInstant(easternTime);
        ZonedDateTime to = now.withHour(9).withMinute(29);
        if (now.isBefore(to) && now.plusMinutes(2).isAfter(to)) Thread.sleep(2*60*1000);
        ibController.requestHistoricalData(ticker, date.format(DateTimeFormatter.ofPattern("YYYYMMdd")), Types.BarSize._30_mins, Types.DurationUnit.MONTH, Types.WhatToShow.TRADES);
        Thread.sleep(10*1000 + 500);
      }
      date = date.plusMonths(1);
    }

    ibController.disconnect();
  }
}
