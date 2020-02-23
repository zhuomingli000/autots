package backtest.utils;

import org.junit.Test;

import java.time.LocalDate;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CalTest {
    @Test
    public void test(){
        assertFalse("new year", Cal.isBusinessDay(LocalDate.of(2013, 1, 1)));
        assertFalse("new year", Cal.isBusinessDay(LocalDate.of(2014, 1, 1)));
        assertFalse("Martin Luther King, Jr. Day", Cal.isBusinessDay(LocalDate.of(2013, 1, 21)));
        assertFalse("Martin Luther King, Jr. Day", Cal.isBusinessDay(LocalDate.of(2014, 1, 20)));
        assertFalse("Washington's Birthday", Cal.isBusinessDay(LocalDate.of(2013, 2, 18)));
        assertFalse("Washington's Birthday", Cal.isBusinessDay(LocalDate.of(2014, 2, 17)));
        assertFalse("Good Friday", Cal.isBusinessDay(LocalDate.of(2013, 3, 29)));
        assertFalse("Good Friday", Cal.isBusinessDay(LocalDate.of(2014, 4, 18)));
        assertFalse("Memorial Day", Cal.isBusinessDay(LocalDate.of(2013, 5, 27)));
        assertFalse("Memorial Day", Cal.isBusinessDay(LocalDate.of(2014, 5, 26)));
        assertFalse("Independence Day",Cal.isBusinessDay(LocalDate.of(2013, 7, 4)));
        assertFalse("Independence Day",Cal.isBusinessDay(LocalDate.of(2014, 7, 4)));
        assertFalse("Labor Day",Cal.isBusinessDay(LocalDate.of(2013, 9, 2)));
        assertFalse("Labor Day",Cal.isBusinessDay(LocalDate.of(2014, 9, 1)));
        assertFalse("Thanksgiving Day",Cal.isBusinessDay(LocalDate.of(2013, 11, 28)));
        assertFalse("Thanksgiving Day",Cal.isBusinessDay(LocalDate.of(2014, 11, 27)));
        assertFalse("Christmas Day",Cal.isBusinessDay(LocalDate.of(2013, 12, 25)));
        assertFalse("Christmas Day",Cal.isBusinessDay(LocalDate.of(2014, 12, 25)));
        assertFalse("weekend", Cal.isBusinessDay(LocalDate.of(2014, 8, 24)));
        assertTrue("normal day",Cal.isBusinessDay(LocalDate.of(2014, 8, 25)));
    }
}
