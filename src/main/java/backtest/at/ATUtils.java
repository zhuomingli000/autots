package backtest.at;

import at.shared.ATServerAPIDefines;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

class ATUtils {
  static ATServerAPIDefines.SYSTEMTIME systemTime(LocalDate date) {
    return systemTime(LocalDateTime.of(date, LocalTime.of(0,0)));
  }

  static ATServerAPIDefines.SYSTEMTIME systemTime(LocalDateTime date) {
    ATServerAPIDefines.SYSTEMTIME systemTime = ActiveTick.apiDefines.new SYSTEMTIME();
    systemTime.year = (short) date.getYear();
    systemTime.month = (short) date.getMonthValue();
    systemTime.day = (short) date.getDayOfMonth();
    systemTime.hour = (short) date.getHour();
    systemTime.minute = (short) date.getMinute();
    systemTime.second = (short) date.getSecond();
    return systemTime;
  }

  static LocalDateTime systemTimeToDateTime(ATServerAPIDefines.SYSTEMTIME systemTime) {
    return LocalDateTime.of(
        systemTime.year,
        systemTime.month,
        systemTime.day,
        systemTime.hour,
        systemTime.minute,
        systemTime.second);
  }
}
