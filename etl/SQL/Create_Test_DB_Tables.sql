CREATE DATABASE test_db;

CREATE TABLE test_db.ticker_history(
	ticker_id INT PRIMARY KEY AUTO_INCREMENT,
    company_id INT NOT NULL,
    tsx_ticker VARCHAR(20) NOT NULL,
    company_name VARCHAR(255) NOT NULL,
    yfinance_ticker VARCHAR(20),
    start_date DATE NOT NULL,
    end_date DATE DEFAULT NULL,
    end_reason VARCHAR(1)
);

CREATE TABLE test_db.company(
	company_id INT PRIMARY KEY AUTO_INCREMENT,
    initial_name VARCHAR(255) NOT NULL,
    date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    date_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);