package backtest.ib;

import backtest.utils.Listenable;
import com.ib.client.Contract;
import com.ib.client.Order;
import com.ib.client.OrderState;
import com.ib.client.OrderStatus;
import com.ib.controller.ApiController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrderHandler extends Listenable<FilledState> implements ApiController.IOrderHandler {
  private static final Logger logger = LoggerFactory.getLogger(OrderHandler.class);
  private boolean isFilled = false;
  private final Contract contract;
  private final Order order;
  private final String name;

  public OrderHandler(Contract contract, Order order, String name) {
    this.contract = contract;
    this.order = order;
    this.name = name;
  }

  public OrderHandler(Contract contract, Order order, String name, Orders orders) {
    this(contract, order, name);
    addListener("for_order_history", orders::record);
  }

  @Override
  public void handle(int errorCode, String errorMsg) {
    logger.error("OrderHandler error: {}, error code: {}, msg: {}", contract.symbol(), errorCode, errorMsg);
  }

  @Override
  public void orderState(OrderState state) {}

  @Override
  public synchronized void orderStatus(OrderStatus status, double filled, double remaining,
                          double avgFillPrice, long permId, int parentId, double lastFillPrice,
                          int clientId, String whyHeld) {
    if (status.equals(OrderStatus.Filled) && !isFilled && Math.abs(remaining)<1e-3) {
      isFilled = true;
      FilledState filledState = new FilledState();
      filledState.contract = contract;
      filledState.filledPrice = avgFillPrice;
      filledState.filledShares = Math.round(filled);
      filledState.orderId = order.orderId();
      filledState.orderName = name;
      notifyListener(filledState);
      logger.info("{}", filledState);
    }
  }
}