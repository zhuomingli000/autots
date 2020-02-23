package backtest.utils;

import java.time.LocalDate;
import java.time.Month;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.HashSet;

/**
 * used to determine whether some day is holiday or not.
 */
public class Cal {
  private static HashMap<Integer, HashSet<LocalDate>> holidayCache = new HashMap<>();

  public static boolean isBusinessDay(LocalDate day) {
    HashSet<LocalDate> holidays = holidayCache.get(day.getYear());
    if (holidays == null) {
      holidays = new HashSet<>();
      int year = day.getYear();
      //New Years Day
      holidays.add(offsetForWeekend(LocalDate.of(year, Month.JANUARY, 1)));
      //Martin Luther King Day
      holidays.add(calculateFloatingHoliday(3, 1, year, 1));
      //Washington's Birthday
      holidays.add(calculateFloatingHoliday(3, 1, year, 2));
      //Good Friday
      holidays.add(getEasterSunday(year).minusDays(2));
      //Memorial Day
      holidays.add(calculateFloatingHoliday(0, 1, year, 5));
      //Independence Day
      holidays.add(offsetForWeekend(LocalDate.of(year, Month.JULY, 4)));
      //Labor Day
      holidays.add(calculateFloatingHoliday(1, 1, year, 9));
      //Thanksgiving Day
      holidays.add(calculateFloatingHoliday(4, 4, year, 11));
      //Christmas
      holidays.add(offsetForWeekend(LocalDate.of(year, Month.DECEMBER, 25)));
      holidayCache.put(day.getYear(), holidays);
    }
    return day.getDayOfWeek().getValue() < 6 && !holidays.contains(day);
  }

  private static LocalDate offsetForWeekend(LocalDate day) {
    if (day.getDayOfWeek().getValue() == 6) return day.minusDays(1);
    else if (day.getDayOfWeek().getValue() == 7) return day.plusDays(1);
    else return day;
  }

  /**
   * @param nth 0 for Last, 1 for 1st, 2 for 2nd, etc.
   */
  private static LocalDate calculateFloatingHoliday(int nth, int dayOfWeek, int year, int month) {
    LocalDate baseDate = LocalDate.of(year, month + (nth > 0 ? 0 : 1), 1);
    int fwd = dayOfWeek - baseDate.getDayOfWeek().getValue();
    return baseDate.plusDays(fwd + (nth - (fwd < 0 ? 0 : 1)) * 7);
  }

  private static LocalDate getEasterSunday(int year) {
    int g = year % 19;
    int c = year / 100;
    int h = (c - (c / 4) - ((8 * c + 13) / 25) + 19 * g + 15) % 30;
    int i = h - h / 28 * (1 - h / 28 * 29 / (h + 1) * (21 - g) / 11);
    int day = i - ((year + year / 4 + i + 2 - c + c / 4) % 7) + 28;
    int month = 3;
    if (day > 31) {
      month++;
      day -= 31;
    }
    return LocalDate.of(year, month, day);
  }

  public static LocalDate getNextBusinessDay(LocalDate day) {
    do {
      day = day.plusDays(1);
    } while (!isBusinessDay(day));
    return day;
  }

  public static LocalDate getLatestBusinessDayBefore(LocalDate day) {
    while (!isBusinessDay(day)) day = day.minusDays(1);
    return day;
  }

  public static LocalDate getLatestBusinessDayAfter(LocalDate day) {
    while (!isBusinessDay(day)) day = day.plusDays(1);
    return day;
  }

  public static LocalDate getPrevBusinessDay(LocalDate day) {
    do {
      day = day.minusDays(1);
    } while (!isBusinessDay(day));
    return day;
  }

  public static LocalDate getPrevBusinessDay(LocalDate day, int j) {
    for (int i = 0; i < j; i++) {
      day = getPrevBusinessDay(day);
    }
    return day;
  }

  public static LocalDate getNextBusinessDay(LocalDate day, int j) {
    for (int i = 0; i < j; i++) {
      day = getNextBusinessDay(day);
    }
    return day;
  }
}
