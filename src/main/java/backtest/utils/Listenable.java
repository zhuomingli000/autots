package backtest.utils;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;

public class Listenable<T> {
  private final ConcurrentMap<String, Consumer<T>> consumers = new ConcurrentHashMap<>();

  public void addListener(String consumerId, Consumer<T> consumer) {
    consumers.put(consumerId, consumer);
  }

  public void removeListener(String consumerId) {
    consumers.remove(consumerId);
  }

  public void notifyListener(T obj) {
    for (Consumer<T> consumer : consumers.values()) {
      consumer.accept(obj);
    }
  }
}
