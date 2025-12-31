-- Setup mysql db
CREATE DATABASE stock_data; -- Set to whatever you would like to name your db

USE stock_data;

CREATE TABLE stock_data.tickers(
	ticker VARCHAR(20) PRIMARY KEY,
    company_name VARCHAR(255) NOT NULL,
    exchange VARCHAR(50) NOT NULL,
    country VARCHAR(20) NOT NULL,
    industry VARCHAR(255) NOT NULL,
    sector VARCHAR(100) NOT NULL,
    reporting_currency VARCHAR(20) NOT NULL,
    `active` VARCHAR(1) DEFAULT 'Y',
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE stock_data.price_history(
	id INT PRIMARY KEY AUTO_INCREMENT,
    `date` DATE NOT NULL,
    ticker VARCHAR(20) NOT NULL,
    `open` FLOAT NOT NULL,
    high FLOAT NOT NULL,
    low FLOAT NOT NULL,
    `close` FLOAT NOT NULL,
    dividends FLOAT NOT NULL,
    stock_splits FLOAT NOT NULL,
    volume INT NOT NULL,
    record_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (ticker) REFERENCES tickers(ticker)
);