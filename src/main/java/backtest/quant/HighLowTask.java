package backtest.quant;

import backtest.regime.RegimeModel;
import backtest.utils.Cal;
import backtest.utils.Canvas;
import backtest.utils.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.time.LocalDate;

public class HighLowTask implements Task {
  private static final Logger logger = LoggerFactory.getLogger(HighLowTask.class);
  private final Stocks stocks;
  private Canvas canvas;

  public HighLowTask(Stocks stocks) {
    this.stocks = stocks;
    canvas = null;
  }

  @Override
  public void perform(LocalDate date) {
    if (!Cal.isBusinessDay(date)) return;
    logger.info("perform high low task");
    stocks.reconnect();
    stocks.updateSP500AndSPY();
    Stock spy = stocks.getStockFromDB("SPY");
    canvas = new Canvas();
    LocalDate from = LocalDate.of(2013, 1, 1);
    LocalDate today = LocalDate.now();
    RegimeModel model = new RegimeModel(stocks);
    model.calcHighMinusLowRate(12, from, today);
    canvas.addSeries(spy.alignWith(model.highMinusLow));
    canvas.addSeries(model.highMinusLow.setName("high minus low rate"), 1);
    canvas.addSeries(model.high, 2);
    canvas.addSeries(model.low, 3);
  }

  public String getHtml() {
    if (canvas == null) perform(LocalDate.now());
    return canvas.toString();
  }

  @Override
  public String toString() {
    return "HighLowTask";
  }
}
