-- Enhanced Database Schema with Constraints and Indexes

-- Companies table with constraints
CREATE TABLE companies (
    company_id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL UNIQUE,
    company_name VARCHAR(255),
    sector VARCHAR(100),
    governance_score DECIMAL(3,2) CHECK (governance_score >= 0 AND governance_score <= 10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_ticker_format CHECK (LENGTH(ticker) <= 10 AND ticker ~ '^[A-Z]+$')
);

-- Stock prices with enhanced constraints
CREATE TABLE stock_prices (
    price_id SERIAL PRIMARY KEY,
    company_id INTEGER NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    date DATE NOT NULL,
    closing_price DECIMAL(10,2) CHECK (closing_price > 0),
    trading_volume BIGINT CHECK (trading_volume >= 0),
    returns DECIMAL(8,6),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(company_id, date)
);

-- SEC filings with validation
CREATE TABLE sec_filings (
    filing_id SERIAL PRIMARY KEY,
    company_id INTEGER NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    filing_date DATE NOT NULL,
    filing_type VARCHAR(10) NOT NULL CHECK (filing_type IN ('8-K', '10-K', '10-Q', '20-F')),
    cybersecurity_mention BOOLEAN DEFAULT FALSE,
    disclosure_speed VARCHAR(20) CHECK (disclosure_speed IN ('Immediate', 'Delayed', 'Unknown')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Cybersecurity incidents
CREATE TABLE cybersecurity_incidents (
    incident_id SERIAL PRIMARY KEY,
    company_id INTEGER NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    breach_date DATE NOT NULL,
    disclosure_date DATE,
    incident_type VARCHAR(50),
    records_affected INTEGER CHECK (records_affected >= 0),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_disclosure_after_breach CHECK (disclosure_date >= breach_date OR disclosure_date IS NULL)
);

-- Performance indexes
CREATE INDEX idx_stock_prices_date ON stock_prices(date);
CREATE INDEX idx_stock_prices_company_date ON stock_prices(company_id, date);
CREATE INDEX idx_sec_filings_date ON sec_filings(filing_date);
CREATE INDEX idx_sec_filings_company ON sec_filings(company_id);
CREATE INDEX idx_incidents_breach_date ON cybersecurity_incidents(breach_date);
CREATE INDEX idx_companies_ticker ON companies(ticker);
CREATE INDEX idx_companies_sector ON companies(sector);

-- Views for common queries
CREAT