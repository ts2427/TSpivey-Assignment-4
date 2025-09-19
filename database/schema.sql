-- Companies table
CREATE TABLE companies (
    company_id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL,
    company_name VARCHAR(255),
    sector VARCHAR(100),
    UNIQUE(ticker)
);

-- Stock prices table  
CREATE TABLE stock_prices (
    price_id SERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES companies(company_id),
    date DATE NOT NULL,
    closing_price DECIMAL(10,2),
    trading_volume BIGINT
);

-- SEC filings table
CREATE TABLE sec_filings (
    filing_id SERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES companies(company_id), 
    filing_date DATE NOT NULL,
    filing_type VARCHAR(10),
    cybersecurity_mention BOOLEAN DEFAULT FALSE
);