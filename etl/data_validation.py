"""
Data quality validation for cybersecurity disclosure dataset
"""

import pandas as pd
import numpy as np

def validate_stock_data(df):
    """Validate stock price data quality"""
    validation_results = {}
    
    # Check for missing values
    validation_results['missing_prices'] = df['closing_price'].isnull().sum()
    validation_results['missing_volume'] = df['trading_volume'].isnull().sum()
    
    # Check for negative prices
    validation_results['negative_prices'] = (df['closing_price'] < 0).sum()
    
    # Check date range
    validation_results['date_range'] = f"{df['date'].min()} to {df['date'].max()}"
    
    return validation_results

def validate_company_data(df):
    """Validate company information"""
    validation_results = {}
    
    # Check for duplicate tickers
    validation_results['duplicate_tickers'] = df['ticker'].duplicated().sum()
    
    # Check for missing company names
    validation_results['missing_names'] = df['company_name'].isnull().sum()
    
    return validation_results

def generate_quality_report(validation_results):
    """Generate data quality report"""
    print("=== Data Quality Report ===")
    for table, results in validation_results.items():
        print(f"\n{table.upper()} Table:")
        for metric, value in results.items():
            print(f"  {metric}: {value}")

if __name__ == "__main__":
    print("Data validation framework ready")