package backtest.quant;

import backtest.ib.IBController;
import backtest.ib.TopMktData;
import backtest.utils.Cal;
import backtest.utils.Email;
import backtest.utils.Task;
import backtest.utils.Util;
import com.ib.client.TickType;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SubscriptionExperiment implements Task {
  private final TopMktData topMktData;
  private final Stocks stocks;
  private final Set<String> doneSet;
  private final Email email;
  private int size;
  private static final ZoneId easternTime = ZoneId.of("America/New_York");

  public SubscriptionExperiment(Stocks stocks, TopMktData topMktData){
    this.topMktData = topMktData;
    this.stocks = stocks;
    this.doneSet = new HashSet<>();
    this.email = new Email();
    this.email.addRecipient("zjhzyyk@gmail.com");
    this.email.alsoPrintToConsole();
  }

  @Override
  public void perform(LocalDate date) {
    if (!Cal.isBusinessDay(date)) return;
    doneSet.clear();
    List<String> sp500AndSPY = stocks.getSP500AndSPY();
    size = sp500AndSPY.size();
    sp500AndSPY.forEach(topMktData::subscribe);
    String listenerId = "experiment";
    topMktData.addListener(listenerId, this::onUpdate);
    Util.sleepTo(9,45);
    topMktData.removeListener(listenerId);
    email.send();
  }

  private synchronized void onUpdate(String ticker, TickType type, Double price) {
    ZonedDateTime now = ZonedDateTime.now().withZoneSameInstant(easternTime);
    ZonedDateTime marketOpen = now.withHour(9).withMinute(30).withSecond(0);
    if (!now.isBefore(marketOpen) && !doneSet.contains(ticker)) {
      doneSet.add(ticker);
      System.out.println("price fetch done for " + doneSet.size() + " tickers");
      if (doneSet.size()==size) {
        email.println(now + ": price fetch done for sp500 and spy");
      }
    }
  }

  public static void main(String[] args) {
    final IBController ibController = new IBController();
    if (!ibController.connect("127.0.0.1", 7497, 6)) return;
    final TopMktData topMktData = new TopMktData(ibController);
    final Stocks stocks = new Stocks();
    SubscriptionExperiment experiment = new SubscriptionExperiment(stocks, topMktData);
    experiment.perform(LocalDate.now());
  }
}
