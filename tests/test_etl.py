"""
Unit tests for ETL pipeline
"""

import unittest
import pandas as pd
from etl.data_validation import validate_stock_data, validate_company_data

class TestETLPipeline(unittest.TestCase):
    
    def test_stock_data_validation(self):
        """Test stock data validation function"""
        # Create sample data
        sample_data = pd.DataFrame({
            'date': ['2023-01-01', '2023-01-02'],
            'closing_price': [100.50, 101.25],
            'trading_volume': [1000000, 1200000]
        })
        
        results = validate_stock_data(sample_data)
        self.assertEqual(results['missing_prices'], 0)
        self.assertEqual(results['negative_prices'], 0)
    
    def test_company_data_validation(self):
        """Test company data validation"""
        sample_companies = pd.DataFrame({
            'ticker': ['AAPL', 'MSFT'],
            'company_name': ['Apple Inc.', 'Microsoft Corp.']
        })
        
        results = validate_company_data(sample_companies)
        self.assertEqual(results['duplicate_tickers'], 0)

if __name__ == '__main__':
    unittest.main()