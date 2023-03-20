CREATE DATABASE  IF NOT EXISTS `dwh`;
USE `dwh`;

DROP TABLE IF EXISTS `facttable`;
DROP TABLE IF EXISTS `customer`;
DROP TABLE IF EXISTS `date`;
DROP TABLE IF EXISTS `product`;
DROP TABLE IF EXISTS `supplier`;
DROP TABLE IF EXISTS `store`;

CREATE TABLE `customer` (
  `CUSTOMER_ID` varchar(4) NOT NULL,
  `CUSTOMER_NAME` varchar(30) NOT NULL,
  PRIMARY KEY (`CUSTOMER_ID`)
);
DROP TABLE IF EXISTS `date`;
CREATE TABLE `date` (
  `date` date NOT NULL,
  `day` varchar(10) NOT NULL,
  `month` int NOT NULL,
  `quarter` int NOT NULL,
  `year` int NOT NULL,
  PRIMARY KEY (`date`)
);
DROP TABLE IF EXISTS `product`;
CREATE TABLE `product` (
  `PRODUCT_ID` varchar(6) NOT NULL,
  `PRODUCT_NAME` varchar(30) NOT NULL,
  PRIMARY KEY (`PRODUCT_ID`)
);

DROP TABLE IF EXISTS `store`;
CREATE TABLE `store` (
  `STORE_ID` varchar(4) NOT NULL,
  `STORE_NAME` varchar(20) NOT NULL,
  PRIMARY KEY (`STORE_ID`)
);

DROP TABLE IF EXISTS `supplier`;
CREATE TABLE `supplier` (
  `SUPPLIER_ID` varchar(5) NOT NULL,
  `SUPPLIER_NAME` varchar(30) NOT NULL,
  PRIMARY KEY (`SUPPLIER_ID`)
);

CREATE TABLE `facttable` (
  `transaction_id` int NOT NULL,
  `product_id` varchar(6) NOT NULL,
  `customer_id` varchar(4) NOT NULL,
  `store_id` varchar(4) NOT NULL,
  `date_id` date NOT NULL,
  `supplier_id` varchar(5) NOT NULL,
  `quantity` smallint NOT NULL,
  `total_sale` decimal(7,2) NOT NULL
);

