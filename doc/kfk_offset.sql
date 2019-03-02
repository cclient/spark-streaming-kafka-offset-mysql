CREATE TABLE `kfk_offset` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `topic` varchar(45) NOT NULL,
  `group` varchar(45) NOT NULL,
  `step` int(11) NOT NULL DEFAULT '0',
  `partition` int(11) NOT NULL,
  `from` bigint(10) NOT NULL,
  `until` bigint(10) NOT NULL,
  `count` bigint(10) NOT NULL DEFAULT '0',
  `datetime` datetime NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `unique` (`topic`,`group`,`step`,`partition`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
