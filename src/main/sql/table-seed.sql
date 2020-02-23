/*
CREATE TABLE `exchange` (
  `id` int NOT NULL AUTO_INCREMENT,
  `abbrev` varchar(32) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

INSERT INTO exchange (abbrev)
VALUES ('NYSE'),('NASDAQ'),('OTHER');
*/

CREATE TABLE `symbol` (
  `id` int NOT NULL AUTO_INCREMENT,
  `ticker` varchar(32) NOT NULL,
  #`exchange_id` int NULL,
  `first_day` date,
  `last_day` date,
  PRIMARY KEY (`id`),
  UNIQUE KEY `index_ticker` (`ticker`)
  #FOREIGN KEY (`exchange_id`) REFERENCES exchange(id) ON DELETE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

CREATE TABLE `daily_price` (
  `id` int NOT NULL AUTO_INCREMENT,
  `symbol_id` int NOT NULL,
  `price_date` date NOT NULL,
  `open_price` decimal(19,4) NULL,
  `high_price` decimal(19,4) NULL,
  `low_price` decimal(19,4) NULL,
  `close_price` decimal(19,4) NULL,
  `adj_close_price` decimal(19,4) NULL,
  `volume` bigint NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `index_symbol_date` (`symbol_id`, `price_date`),
  FOREIGN KEY `index_symbol_id` (`symbol_id`) REFERENCES symbol(id) ON DELETE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

/*
CREATE TABLE `sp500` (
  `id` int NOT NULL AUTO_INCREMENT,
  `symbol_id` int NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `index_symbol_id` (`symbol_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;
*/