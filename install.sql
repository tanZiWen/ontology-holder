CREATE TABLE `holder` (
  `address` varchar(48) NOT NULL,
  `contract` varchar(48) NOT NULL,
  `balance` bigint(19) NOT NULL,
  PRIMARY KEY (`address`,`contract`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `eventnotify` (
  `tx_hash` varchar(64) NOT NULL,
  `height` int(10) unsigned NOT NULL,
  `state` tinyint(3) unsigned zerofill NOT NULL,
  `gas_consumed` bigint(19) unsigned zerofill NOT NULL,
  `notify` json DEFAULT NULL,
  PRIMARY KEY (`tx_hash`),
  UNIQUE KEY `tx_hash_UNIQUE` (`tx_hash`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `heartbeat` (
  `module` varchar(64) NOT NULL,
  `node_id` int(11) NOT NULL,
  `update_time` datetime NOT NULL,
  PRIMARY KEY (`module`),
  UNIQUE KEY ` node_id_UNIQUE` (`module`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;