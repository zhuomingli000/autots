package backtest;

import backtest.ib.Account;
import backtest.ib.IBController;
import backtest.ib.Orders;
import backtest.quant.GapTask;
import backtest.utils.*;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

import static spark.Spark.get;
import static spark.Spark.port;

public class Server {
//  private static final Logger logger = LoggerFactory.getLogger(Server.class);
  public static void main(String[] args) {
    final Orders orders = new Orders();
    final IBController ibController = new IBController();
    int retry = 1;
    int maxRetry = 2;
    while (!ibController.connect("127.0.0.1", 7497, 3)) {
      retry++;
      if (retry > maxRetry) return;
    }
    final Account account = new Account(ibController);
    final Email email = new Email();
    email.alsoPrintToConsole();
    email.addRecipient(getEmail());
    Scheduler scheduler = new Scheduler();
    scheduler.schedule(new GapTask(account, ibController, orders, email), 7, 0);
    port(8080);
    get("/", (req, res) -> {
      String accountSummary = account.getAccountSummary();
      String orderHistory =  orders.getHistory();
      String ret = orderHistory.isEmpty() ? accountSummary : accountSummary + "\n\n" + orderHistory;
      return ret.replaceAll("\n", "<br>");
    });
  }

private static String getEmail() {
	return "autost@gmail.com";
}
}
