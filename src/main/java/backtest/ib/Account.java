package backtest.ib;

import backtest.struct.ArrayMap;
import backtest.utils.Cal;
import com.ib.controller.ApiController;
import com.ib.controller.Position;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class Account implements ApiController.IAccountHandler {
  private static final Logger logger = LoggerFactory.getLogger(Account.class);
  private CountDownLatch latch;
  private double buyingPower;
  private double netLiquid;
  private double cashBalance;
  private ConcurrentMap<Integer, Position> positions;
  private ArrayMap<LocalDate, Map<Integer, Position>> positionHistory;
  private final Object historyLock;

  public Account(IBController ibController) {
    this.positions = new ConcurrentHashMap<>();
    this.buyingPower = 0;
    this.netLiquid = 0;
    this.positionHistory = new ArrayMap<>();
    this.historyLock = new Object();

    latch = new CountDownLatch(1);
    ibController.reqAccountUpdates(this);
    try {
      if (!latch.await(5, TimeUnit.SECONDS)) {
        logger.error("account update request times out after 5 seconds");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.error("Account update req interrupted.");
    }
  }

  @Override
  public void updatePortfolio(Position position) {
    if (Math.abs(position.position()) < 1e-3) {
      positions.remove(position.conid());
      LocalDate today = LocalDate.now();
      synchronized (historyLock) {
        if (positionHistory.size()==0 || positionHistory.lastKey().isBefore(today)) {
          Map<Integer, Position> map = new HashMap<>();
          map.put(position.conid(), position);
          positionHistory.put(today, map);
        } else {
          Map<Integer, Position> map = positionHistory.lastValue();
          map.put(position.conid(), position);
          positionHistory.setValue(positionHistory.size()-1, map);
        }
        while (positionHistory.size()!=0 &&
            positionHistory.firstKey().isBefore(Cal.getPrevBusinessDay(today, 2))) {
          positionHistory.removeFirst();
        }
      }
    } else {
      positions.put(position.conid(), position);
    }
  }

  @Override
  public void accountValue(String account, String key, String value,
                           String currency) {
    if (key.equals("BuyingPower")) {
      buyingPower = Double.valueOf(value);
    }
    if (key.equals("NetLiquidation")) {
      netLiquid = Double.valueOf(value);
    }
    if (key.equals("CashBalance")) {
      cashBalance = Double.valueOf(value);
    }
  }

  public double getBuyingPower(){return buyingPower;}

  public Map<Integer, Position> getPositions(){return positions;}

  @Override
  public void accountTime(String timeStamp) {}

  @Override
  public void accountDownloadEnd(String account) {
    latch.countDown();
  }

  public double getNetLiquid() {
    return netLiquid;
  }

  public String getPositionHistory() {
    synchronized (historyLock) {
      if (positionHistory.size() == 0) {
        return "";
      }
      String ret = "Position history:\n";
      for (int i = positionHistory.size() - 1; i >= 0; i--) {
        Map.Entry<LocalDate, Map<Integer, Position>> entry = positionHistory.getEntry(i);
        ret += "\n" + entry.getKey() + "\n";
        double totalRealPnl = 0;
        String day = "";
        for (Map.Entry<Integer, Position> p : entry.getValue().entrySet()) {
          day += "\n" + posToStr(p.getValue());
          totalRealPnl += p.getValue().realPnl();
        }
        ret += "Total realized P&L: " + totalRealPnl + "\n" + day;
      }
      return ret;
    }
  }

  public String posToStr(Position position) {
    String ret = position.contract().symbol() + "\n";
    ret += "contract id: " + position.contract().conid() + "\n";
    ret += "position: " + position.position() + "\n";
    ret += "market price: " + position.marketPrice() + "\n";
    ret += "market value: " + position.marketValue() + "\n";
    ret += "avg cost: " + position.averageCost() + "\n";
    ret += "unrealized P&L: " + position.unrealPnl() + "\n";
    ret += "realized P&L: " + position.realPnl() + "\n";
    return ret;
  }

  public String getCurPosSummary() {
    String currentPositions = "portfolio contains " + positions.size() + " stocks\n";
    for (Map.Entry<Integer, Position> entry : positions.entrySet()) {
      Position position = entry.getValue();
      currentPositions += "\n" + posToStr(position);
    }
    return currentPositions;
  }

  public double getTotalUnrealPnl() {
    double totalUnrealPnl = 0;
    for (Map.Entry<Integer, Position> entry : positions.entrySet()) {
      totalUnrealPnl += entry.getValue().unrealPnl();
    }
    return totalUnrealPnl;
  }

  // total realized pnl of today.
  public double getTotalRealPnl() {
    synchronized (historyLock) {
      if (positionHistory.size() == 0 || positionHistory.lastKey().isBefore(LocalDate.now())) {
        return 0;
      }
      Map<Integer, Position> realPos = positionHistory.lastValue();
      double totalRealPnl = 0;
      for (Map.Entry<Integer, Position> p : realPos.entrySet()) {
        totalRealPnl += p.getValue().realPnl();
      }
      return totalRealPnl;
    }
  }

  public String getAccountSummary() {
    String ret = "Buying power: " + getBuyingPower() + "\n";
    ret += "Net liquidation: " + getNetLiquid() + "\n";
    ret += "Cash balance: " + cashBalance + "\n";
    ret += "Total unreal PnL: " + getTotalUnrealPnl() + "\n";
    ret += "Total real PnL: " + getTotalRealPnl() + "\n";
    ret += getCurPosSummary();
    ret += "\n" + getPositionHistory();
    return ret;
  }
}
