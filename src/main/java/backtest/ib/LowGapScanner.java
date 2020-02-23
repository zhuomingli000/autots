package backtest.ib;

import backtest.utils.Listenable;
import com.ib.client.Contract;
import com.ib.client.ContractDetails;
import com.ib.controller.ApiController;

import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class LowGapScanner extends Listenable<ConcurrentNavigableMap<Integer, Contract>>
    implements ApiController.IScannerHandler{
  private final IBController ibController;
  private ConcurrentNavigableMap<Integer, Contract> scannerData;

  public LowGapScanner(IBController ibController) {
    scannerData = new ConcurrentSkipListMap<>();
    this.ibController = ibController;
  }

  @Override
  public void scannerParameters(String xml) {}

  @Override
  public void scannerData(int rank, ContractDetails contractDetails, String legsStr) {
    scannerData.put(rank, contractDetails.contract().clone());
  }

  @Override
  public void scannerDataEnd() {
    notifyListener(scannerData);
    scannerData.clear();
  }

  public void subscribe() {
    ibController.scanLowOpenGap(this);
  }

  public void unsubscribe() {
    ibController.unsubscribeScanner(this);
  }

  public static void main(String[] args) {
    IBController ibController = new IBController();
    if (!ibController.connect("127.0.0.1", 7497, 5)) return;
    LowGapScanner scanner = new LowGapScanner(ibController);
    scanner.subscribe();
    scanner.addListener("scanner_main", d -> {
      for (Map.Entry<Integer, Contract> entry : d.entrySet()) {
        System.out.println(entry.getKey() + ": " + entry.getValue().symbol());
      }
      scanner.unsubscribe();
      ibController.disconnect();
    });
  }
}
