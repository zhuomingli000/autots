package backtest.utils;

import java.time.LocalDate;

public interface Task {
  void perform(LocalDate date);
}