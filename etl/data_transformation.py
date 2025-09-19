"""
Data transformation logic for cybersecurity disclosure analysis
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def calculate_returns(stock_data):
    """Calculate daily returns from stock prices"""
    try:
        stock_data = stock_data.sort_values(['company_id', 'date'])
        stock_data['returns'] = stock_data.groupby('company_id')['closing_price'].pct_change()
        logger.info(f"Calculated returns for {len(stock_data)} records")
        return stock_data
    except Exception as e:
        logger.error(f"Error calculating returns: {e}")
        return None

def classify_disclosure_speed(incidents_df, filings_df):
    """Classify disclosure timing as immediate vs delayed"""
    try:
        merged = incidents_df.merge(
            filings_df, 
            on='company_id', 
            how='left'
        )
        
        # Calculate days between breach and disclosure
        merged['days_to_disclosure'] = (
            pd.to_datetime(merged['filing_date']) - 
            pd.to_datetime(merged['breach_date'])
        ).dt.days
        
        # Classify speed (immediate = within 4 business days)
        merged['disclosure_speed'] = np.where(
            merged['days_to_disclosure'] <= 4, 
            'Immediate', 
            'Delayed'
        )
        
        logger.info(f"Classified disclosure speed for {len(merged)} incidents")
        return merged
        
    except Exception as e:
        logger.error(f"Error classifying disclosure speed: {e}")
        return None

def detect_cybersecurity_mentions(filing_text):
    """Detect cybersecurity-related keywords in SEC filings"""
    cyber_keywords = [
        'cybersecurity', 'cyber security', 'data breach', 'hacking', 
        'malware', 'ransomware', 'phishing', 'unauthorized access',
        'security incident', 'data theft', 'privacy breach'
    ]
    
    if pd.isna(filing_text):
        return False
        
    text_lower = filing_text.lower()
    return any(keyword in text_lower for keyword in cyber_keywords)

def clean_company_data(companies_df):
    """Clean and standardize company information"""
    try:
        # Remove duplicates
        companies_df = companies_df.drop_duplicates(subset=['ticker'])
        
        # Standardize ticker format
        companies_df['ticker'] = companies_df['ticker'].str.upper().str.strip()
        
        # Clean company names
        companies_df['company_name'] = companies_df['company_name'].str.strip()
        
        # Validate governance scores
        companies_df['governance_score'] = pd.to_numeric(
            companies_df['governance_score'], 
            errors='coerce'
        )
        
        logger.info(f"Cleaned data for {len(companies_df)} companies")
        return companies_df
        
    except Exception as e:
        logger.error(f"Error cleaning company data: {e}")
        return None

def calculate_event_study_windows(incident_date, stock_data, window_days=3):
    """Calculate event study windows around cybersecurity incidents"""
    try:
        incident_date = pd.to_datetime(incident_date)
        
        # Define event window
        start_date = incident_date - timedelta(days=window_days)
        end_date = incident_date + timedelta(days=window_days)
        
        # Filter stock data to event window
        event_window = stock_data[
            (stock_data['date'] >= start_date) & 
            (stock_data['date'] <= end_date)
        ].copy()
        
        # Mark event day
        event_window['event_day'] = (event_window['date'] == incident_date)
        
        return event_window
        
    except Exception as e: