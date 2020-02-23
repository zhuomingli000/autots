package backtest.ib;

@FunctionalInterface
public interface TopMktDataConsumer<String, TickType, Double> {
  void process(String ticker, TickType tickType, Double price);
}
