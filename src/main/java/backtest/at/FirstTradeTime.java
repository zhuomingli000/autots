package backtest.at;

import backtest.io.SQLite;
import backtest.quant.Stocks;
import backtest.struct.TimeSeries;
import backtest.utils.Cal;
import backtest.utils.Canvas;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;

// Gives the time when open price of all stocks are available
public class FirstTradeTime {
  private static final Logger logger = LoggerFactory.getLogger(FirstTradeTime.class);

  public static void main(String[] args) {
    try (SQLite ohlc = new SQLite("ohlc.db");
         SQLite trades = new SQLite("trades.db");
         SQLite lastQuotesDB = new SQLite("last_quotes.db");
         ActiveTick activeTick = new ActiveTick()) {
      DayRanges ranges = new DayRanges(ohlc);
      BarHistory ohlcGetter = new BarHistory(ohlc, activeTick, ranges);

      final double stopLoss = 1;
      LocalDate from = Cal.getLatestBusinessDayAfter(LocalDate.of(2016, 8, 19));
      LocalDate to = LocalDate.of(2016, 8, 24);
      List<String> sp500 = Stocks.getSP500Online();
      Trade.createSQLiteTableIfNotExist(trades);
      TimeRanges tradeRanges = new TimeRanges(trades);
      TimeRanges lastQuotesRanges = new TimeRanges(lastQuotesDB);
      LastQuotes lastQuotes = new LastQuotes(lastQuotesDB, activeTick, lastQuotesRanges);

      // fetch and read first trades from 9:30 to 9:35
      logger.info("read first trades");
      Map<LocalDate, List<Trade>> firstTrades = new HashMap<>();
      for (LocalDate date = from; date.isBefore(to); date = Cal.getNextBusinessDay(date)) {
        List<Trade> tradeList = new ArrayList<>();
        for (String ticker : sp500) {
          LocalDateTime start = LocalDateTime.of(date, LocalTime.of(9, 30));
          if (!tradeRanges.contains(ticker, start)) {
            Trade.getOrFetch(trades, activeTick, ticker, start, tradeRanges);
          }
          Optional<Trade> optTrade = Trade.firstTradeBefore(trades, ticker, date, LocalTime.of(9, 40));
          if (optTrade.isPresent()) tradeList.add(optTrade.get());
        }
        firstTrades.put(date, tradeList);
      }

      logger.info("start iterating endTimes");
      Canvas canvas = new Canvas("output/perf.html");
      // end time in reverse order so only first endtime fetch last quotes in last 5 minutes.
      List<LocalTime> endTimes = ImmutableList.of(
//          LocalTime.of(9, 31),
//          LocalTime.of(9, 30, 30),
          LocalTime.of(9, 30, 15)
//          LocalTime.of(9, 30, 10),
//          LocalTime.of(9, 30, 5),
//          LocalTime.of(9, 30, 1)
      );
      for (LocalTime endTime : endTimes) {
        System.out.println("endtime: " + endTime);
        double totalProfit = 0;
        TimeSeries profitSeries = new TimeSeries();
        for (LocalDate date = from; date.isBefore(to); date = Cal.getNextBusinessDay(date)) {
          List<Gap> gaps = new ArrayList<>();
          LocalDate lastDay = Cal.getPrevBusinessDay(date);

          // calc gaps for all trades before end time.
          for (Trade trade : firstTrades.get(date)) {
            if (trade.time.toLocalTime().isBefore(endTime)) {
              Gap gap = new Gap();
              gap.ticker = trade.ticker;
              double lastClose;
              Optional<OHLC> optOhlc = ohlcGetter.getOrFetch(trade.ticker, lastDay, to);
              if (!optOhlc.isPresent()) {
//                System.out.println("no last close for " + trade.ticker + " on " + lastDay);
                continue;
              } else {
                lastClose = optOhlc.get().close;
              }
              gap.gap = (trade.price - lastClose) / lastClose;
              gaps.add(gap);
            }
          }

          System.out.println(date);
          double dayProfit = 0;
          double n = 0;
          for (Gap gap : Ordering.natural().leastOf(gaps, 6)) {
            logger.info(gap.ticker + ": " + gap.gap);
            LocalDateTime toTime = LocalDateTime.of(date, endTime);
            Optional<Quote> lastQuote = lastQuotes.getOrFetchLastQuoteBefore(gap.ticker, toTime);
            if (lastQuote.isPresent()) {
              double buyPrice = (lastQuote.get().bidPrice + lastQuote.get().askPrice) / 2;
              Optional<OHLC> optOhlc = ohlcGetter.getOrFetch(gap.ticker, date, to);
              if (optOhlc.isPresent()) {
                double sellPrice;
                if ((optOhlc.get().low-buyPrice)/buyPrice < -stopLoss) {
                  sellPrice = buyPrice*(1-stopLoss);
                } else {
                  sellPrice = optOhlc.get().close;
                }
                double profit = (sellPrice-buyPrice)/buyPrice;
                logger.info("{}, buy: {}, sell: {}", gap.ticker, buyPrice, sellPrice);
                logger.info("profit of {}: {}", gap.ticker, profit);
                dayProfit += profit;
                n += 1;
              } else {
                logger.error("{} doesn't have ohlc data in {}", gap.ticker, date);
              }
            } else {
              logger.error("{} doesn't have quote from {} to {}", gap.ticker, toTime.minusMinutes(5), toTime);
            }
          }
          if (n < 0.01) {
            logger.error("no gap ticker traded in {}", date);
          } else {
            logger.info("day profit: {}: {}", date, dayProfit/n);
            totalProfit += dayProfit/n;
          }
          System.out.println();
          profitSeries.put(date, totalProfit);
        }
        logger.info("total profit: {}", totalProfit);
        canvas.addSeries(profitSeries.setName(endTime.toString()));
      }
      Stocks stocks = new Stocks();
      stocks.updateTo(LocalDate.now(), "SPY");
      canvas.addSeries(stocks.getStockFromDB("SPY").toSeries(from, to), 1);
//      canvas.draw();
    } catch (ConnectionException e) {
      logger.info("connection fails");
    }
  }
}