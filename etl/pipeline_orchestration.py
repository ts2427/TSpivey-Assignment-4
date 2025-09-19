"""
Main ETL pipeline orchestration for cybersecurity disclosure analysis
"""

import pandas as pd
import logging
from datetime import datetime
import sys
import os

# Import our modules
from data_extraction import connect_to_wrds, extract_stock_data, extract_sec_filings
from data_transformation import (
    calculate_returns, classify_disclosure_speed, 
    clean_company_data, detect_cybersecurity_mentions
)
from data_validation import validate_stock_data, validate_company_data, generate_quality_report

# Set up comprehensive logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class ETLPipeline:
    """Main ETL pipeline orchestrator"""
    
    def __init__(self, config=None):
        self.config = config or {}
        self.wrds_connection = None
        self.validation_results = {}
        
    def run_full_pipeline(self, start_date='2020-01-01', end_date='2024-12-31'):
        """Execute complete ETL pipeline"""
        try:
            logger.info("Starting ETL pipeline execution")
            
            # Step 1: Extract data
            logger.info("Step 1: Data Extraction")
            raw_data = self.extract_data(start_date, end_date)
            
            if not raw_data:
                logger.error("Data extraction failed - stopping pipeline")
                return False
                
            # Step 2: Transform data
            logger.info("Step 2: Data Transformation")
            transformed_data = self.transform_data(raw_data)
            
            if not transformed_data:
                logger.error("Data transformation failed - stopping pipeline")
                return False
                
            # Step 3: Validate data quality
            logger.info("Step 3: Data Validation")
            validation_passed = self.validate_data(transformed_data)
            
            if not validation_passed:
                logger.warning("Data validation issues detected - review required")
                
            # Step 4: Load to database
            logger.info("Step 4: Data Loading")
            load_success = self.load_data(transformed_data)
            
            if load_success:
                logger.info("ETL pipeline completed successfully")
                return True
            else:
                logger.error("Data loading failed")
                return False
                
        except Exception as e:
            logger.error(f"Pipeline execution failed: {e}")
            return False
        finally:
            self.cleanup()
    
    def extract_data(self, start_date, end_date):
        """Coordinate data extraction from all sources"""
        try:
            extracted_data = {}
            
            # Connect to WRDS
            self.wrds_connection = connect_to_wrds()
            if not self.wrds_connection:
                logger.error("Failed to connect to WRDS")
                return None
                
            # Extract stock data
            logger.info("Extracting stock data from WRDS")
            stock_data = extract_stock_data(self.wrds_connection, start_date, end_date)
            extracted_data['stock_data'] = stock_data
            
            # Extract SEC filings
            logger.info("Extracting SEC filing data")
            sec_data = extract_sec_filings()
            extracted_data['sec_filings'] = sec_data
            
            # Extract cybersecurity incidents (placeholder)
            logger.info("Extracting cybersecurity incident data")
            # This would connect to Privacy Rights Clearinghouse or similar
            extracted_data['incidents'] = pd.DataFrame()  # Placeholder
            
            logger.info(f"Data extraction completed - {len(extracted_data)} datasets")
            return extracted_data
            
        except Exception as e:
            logger.error(f"Data extraction error: {e}")
            return None
    
    def transform_data(self, raw_data):
        """Apply all data transformations"""
        try:
            transformed_data = {}
            
            # Transform stock data
            if 'stock_data' in raw_data and raw_data['stock_data'] is not None:
                logger.info("Transforming stock data")
                stock_with_returns = calculate_returns(raw_data['stock_data'])
                transformed_data['stock_data'] = stock_with_returns
            
            # Process SEC filings for cybersecurity mentions
            if 'sec_filings' in raw_data and raw_data['sec_filings'] is not None:
                logger.info("Processing SEC filings")
                sec_data = raw_data['sec_filings'].copy()
                # Apply cybersecurity detection if filing text available
                if 'filing_text' in sec_data.columns:
                    sec_data['cybersecurity_mention'] = sec_data['filing_text'].apply(
                        detect_cybersecurity_mentions
                    )
                transformed_data['sec_filings'] = sec_data
            
            # Classify disclosure timing
            if ('incidents' in raw_data and 'sec_filings' in transformed_data):
                logger.info("Classifying disclosure timing")
                disclosure_analysis = classify_disclosure_speed(
                    raw_data['incidents'], 
                    transformed_data['sec_filings']
                )
                transformed_data['disclosure_analysis'] = disclosure_analysis
            
            logger.info("Data transformation completed")
            return transformed_data
            
        except Exception as e:
            logger.error(f"Data transformation error: {e}")
            return None
    
    def validate_data(self, transformed_data):
        """Run comprehensive data validation"""
        try:
            validation_passed = True
            
            for dataset_name, dataset in transformed_data.items():
                if dataset is None or dataset.empty:
                    logger.warning(f"Empty dataset: {dataset_name}")
                    continue
                    
                logger.info(f"Validating {dataset_name}")
                
                if dataset_name == 'stock_data':
                    results = validate_stock_data(dataset)
                    self.validation_results[dataset_name] = results
                    
                    # Check validation thresholds
                    if results['missing_prices'] > len(dataset) * 0.05:  # > 5% missing
                        logger.warning(f"High missing price data: {results['missing_prices']}")
                        validation_passed = False
            
            # Generate validation report
            generate_quality_report(self.validation_results)
            
            return validation_passed
            
        except Exception as e:
            logger.error(f"Data validation error: {e}")
            return False
    
    def load_data(self, transformed_data):
        """Load data to database"""
        try:
            # This would implement actual database loading
            # For now, save to files as proof of concept
            
            for dataset_name, dataset in transformed_data.items():
                if dataset is not None and not dataset.empty:
                    output_file = f"output_{dataset_name}_{datetime.now().strftime('%Y%m%d')}.csv"
                    dataset.to_csv(output_file, index=False)
                    logger.info(f"Saved {dataset_name} to {output_file}")
            
            logger.info("Data loading completed")
            return True
            
        except Exception as e:
            logger.error(f"Data loading error: {e}")
            return False
    
    def cleanup(self):
        """Clean up resources"""
        try:
            if self.wrds_connection:
                self.wrds_connection.close()
                logger.info("WRDS connection closed")
        except Exception as e:
            logger.error(f"Cleanup error: {e}")

if __name__ == "__main__":
    # Run pipeline
    pipeline = ETLPipeline()
    success = pipeline.run_full_pipeline()
    
    if success:
        print("ETL pipeline completed successfully")
        sys.exit(0)
    else:
        print("ETL pipeline failed")
        sys.exit(1)