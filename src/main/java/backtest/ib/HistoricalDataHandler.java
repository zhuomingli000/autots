package backtest.ib;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import com.ib.controller.ApiController.IHistoricalDataHandler;
import com.ib.controller.Bar;

public class HistoricalDataHandler implements IHistoricalDataHandler {
  
  String ticker;
  
  public HistoricalDataHandler(String ticker) {
    this.ticker = ticker;
  }

  @Override
  public void historicalData(Bar bar, boolean hasGaps) {
    String output = ticker + " " + bar.toString() + " " + bar.volume() + "\n";
    String fileString = "data/"+ticker+".txt";
    File file = new File(fileString);
    if (!file.exists())
      try {
        file.createNewFile();
      } catch (IOException e1) {
        // TODO Auto-generated catch block
        e1.printStackTrace();
      }
    try {
      Files.write(Paths.get(fileString), output.getBytes(), StandardOpenOption.APPEND);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    System.out.println("pulled " + ticker + " " + bar.toString());
  }

  @Override
  public void historicalDataEnd() {
  }

}
