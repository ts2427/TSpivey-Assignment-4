"""
Advanced data validation using Pandera framework
"""

import pandas as pd
import pandera as pa
from pandera import Column, DataFrameSchema, Check
import logging

logger = logging.getLogger(__name__)

# Define schemas for each table
companies_schema = DataFrameSchema({
    "ticker": Column(str, checks=[
        Check.str_matches(r'^[A-Z]{1,10}$'),  # 1-10 uppercase letters
        Check(lambda x: x.str.len() <= 10)
    ]),
    "company_name": Column(str, checks=[
        Check.str_length(min_val=1, max_val=255),
        Check(lambda x: ~x.str.contains(r'[<>]'))  # No HTML tags
    ]),
    "sector": Column(str, nullable=True, checks=[
        Check.str_length(max_val=100)
    ]),
    "governance_score": Column(float, nullable=True, checks=[
        Check.between(0, 10)
    ])
})

stock_prices_schema = DataFrameSchema({
    "company_id": Column(int, checks=[Check.greater_than(0)]),
    "date": Column(pd.Timestamp),
    "closing_price": Column(float, checks=[
        Check.greater_than(0),
        Check.less_than(10000)  # Reasonable upper bound
    ]),
    "trading_volume": Column(int, checks=[
        Check.greater_than_or_equal_to(0),
        Check.less_than(1e12)  # Reasonable upper bound
    ]),
    "returns": Column(float, nullable=True, checks=[
        Check.between(-1, 1)  # Daily returns shouldn't exceed 100%
    ])
})

sec_filings_schema = DataFrameSchema({
    "company_id": Column(int, checks=[Check.greater_than(0)]),
    "filing_date": Column(pd.Timestamp),
    "filing_type": Column(str, checks=[
        Check.isin(['8-K', '10-K', '10-Q', '20-F'])
    ]),
    "cybersecurity_mention": Column(bool),
    "disclosure_speed": Column(str, nullable=True, checks=[
        Check.isin(['Immediate', 'Delayed', 'Unknown'])
    ])
})

cybersecurity_incidents_schema = DataFrameSchema({
    "company_id": Column(int, checks=[Check.greater_than(0)]),
    "breach_date": Column(pd.Timestamp),
    "disclosure_date": Column(pd.Timestamp, nullable=True),
    "incident_type": Column(str, nullable=True),
    "records_affected": Column(int, nullable=True, checks=[
        Check.greater_than_or_equal_to(0)
    ])
})

class DataValidator:
    """Advanced data validation using Pandera schemas"""
    
    def __init__(self):
        self.schemas = {
            'companies': companies_schema,
            'stock_prices': stock_prices_schema,
            'sec_filings': sec_filings_schema,
            'cybersecurity_incidents': cybersecurity_incidents_schema
        }
    
    def validate_dataset(self, df, dataset_name):
        """Validate a dataset against its schema"""
        try:
            if dataset_name not in self.schemas:
                logger.warning(f"No schema defined for {dataset_name}")
                return True, []
            
            schema = self.schemas[dataset_name]
            validated_df = schema.validate(df, lazy=True)
            
            logger.info(f"✓ {dataset_name} validation passed ({len(df)} records)")
            return True, []
            
        except pa.errors.SchemaErrors as e:
            logger.error(f"✗ {dataset_name} validation failed")
            
            errors = []
            for error in e.schema_errors:
                error_msg = f"{error.schema}: {error.message}"
                errors.append(error_msg)
                logger.error(f"  - {error_msg}")
            
            return False, errors
    
    def validate_cross_table_constraints(self, datasets):
        """Validate constraints across multiple tables"""
        errors = []
        
        try:
            # Check referential integrity
            if 'companies' in datasets and 'stock_prices' in datasets:
                companies_df = datasets['companies']
                stock_df = datasets['stock_prices']
                
                # Check all company_ids in stock_prices exist in companies
                valid_company_ids = set(companies_df['company_id'])
                stock_company_ids = set(stock_df['company_id'])
                
                invalid_ids = stock_company_ids - valid_company_ids
                if invalid_ids:
                    errors.append(f"Stock prices reference invalid company_ids: {invalid_ids}")
            
            # Check temporal consistency
            if 'cybersecurity_incidents' in datasets:
                incidents_df = datasets['cybersecurity_incidents']
                
                # Disclosure date should be after breach date
                invalid_dates = incidents_df[
                    (incidents_df['disclosure_date'].notna()) &
                    (incidents_df['disclosure_date'] < incidents_df['breach_date'])
                ]
                
                if len(invalid_dates) > 0:
                    errors.append(f"Found {len(invalid_dates)} incidents with disclosure before breach")
            
            return len(errors) == 0, errors
            
        except Exception as e:
            logger.error(f"Cross-table validation error: {e}")
            return False, [str(e)]
    
    def generate_data_profile(self, df, dataset_name):
        """Generate comprehensive data profile"""
        profile = {
            'dataset': dataset_name,
            'record_count': len(df),
            'column_count': len(df.columns),
            'memory_usage_mb': df.memory_usage(deep=True).sum() / 1024 / 1024,
            'null_counts': df.isnull().sum().to_dict(),
            'duplicate_rows': df.duplicated().sum()
        }
        
        # Add column-specific statistics
        numeric_cols = df.select_dtypes(include=['number']).columns
        for col in numeric_cols:
            profile[f'{col}_stats'] = {
                'mean': df[col].mean(),
                'std': df[col].std(),
                'min': df[col].min(),
                'max': df[col].max(),
                'outliers': len(df[df[col] > df[col].quantile(0.95)])
            }
        
        return profile

def run_comprehensive_validation(datasets):
    """Run full validation suite on all datasets"""
    validator = DataValidator()
    validation_report = {
        'timestamp': pd.Timestamp.now(),
        'overall_status': 'PASSED',
        'dataset_results': {},
        'cross_table_results': {},
        'data_profiles': {}
    }
    
    # Validate each dataset
    for name, df in datasets.items():
        if df is not None and not df.empty:
            passed, errors = validator.validate_dataset(df, name)
            validation_report['dataset_results'][name] = {
                'passed': passed,
                'errors': errors,
                'record_count': len(df)
            }
            
            if not passed:
                validation_report['overall_status'] = 'FAILED'
            
            # Generate data profile
            validation_report['data_profiles'][name] = validator.generate_data_profile(df, name)
    
    # Cross-table validation
    passed, errors = validator.validate_cross_table_constraints(datasets)
    validation_report['cross_table_results'] = {
        'passed': passed,
        'errors': errors
    }
    
    if not passed:
        validation_report['overall_status'] = 'FAILED'
    
    return validation_report

if __name__ == "__main__":
    logger.info("Advanced validation module ready")