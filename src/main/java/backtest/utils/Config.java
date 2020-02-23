package backtest.utils;

import java.io.IOException;
import java.util.Properties;

public class Config {
  private static Config instance = new Config();
  private Properties prop;

  public static Config getInstance() {
    return instance;
  }

  private Config() {
    prop = new Properties();
    ClassLoader classLoader = getClass().getClassLoader();
    try {
      prop.load(classLoader.getResourceAsStream("config.properties"));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public String get(String key) {
    return prop.getProperty(key);
  }
}
