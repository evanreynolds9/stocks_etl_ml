-- Setup mysql db
CREATE DATABASE stocks_etl_ml_db;

USE stocks_etl_ml_db;

-- Create table used to store S&P tickers
CREATE TABLE SP_Constituents (
	id INT AUTO_INCREMENT PRIMARY KEY,
    Actual_Date DATETIME,
    Load_Date DATETIME,
    Tickers VARCHAR(50)
);