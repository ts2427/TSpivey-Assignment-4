"""
Comprehensive error handling and logging utilities
"""

import logging
import traceback
import functools
from datetime import datetime
import smtplib
from email.mime.text import MIMEText

class ETLError(Exception):
    """Custom exception for ETL pipeline errors"""
    pass

class DataQualityError(ETLError):
    """Exception for data quality issues"""
    pass

class ConnectionError(ETLError):
    """Exception for database/API connection issues"""
    pass

def setup_logging(log_level=logging.INFO, log_file='etl_pipeline.log'):
    """Set up comprehensive logging configuration"""
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
    )
    
    # File handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)
    
    # Root logger
    logger = logging.getLogger()
    logger.setLevel(log_level)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

def retry_on_failure(max_retries=3, delay=1):
    """Decorator to retry functions on failure"""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries:
                        logging.warning(f"Attempt {attempt + 1} failed for {func.__name__}: {e}")
                        time.sleep(delay * (2 ** attempt))  # Exponential backoff
                    else:
                        logging.error(f"All {max_retries + 1} attempts failed for {func.__name__}")
            
            raise last_exception
        return wrapper
    return decorator

def log_execution_time(func):
    """Decorator to log function execution time"""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = datetime.now()
        logger = logging.getLogger(func.__module__)
        
        try:
            logger.info(f"Starting {func.__name__}")
            result = func(*args, **kwargs)
            
            execution_time = (datetime.now() - start_time).total_seconds()
            logger.info(f"Completed {func.__name__} in {execution_time:.2f} seconds")
            
            return result
            
        except Exception as e:
            execution_time = (datetime.now() - start_time).total_seconds()
            logger.error(f"Failed {func.__name__} after {execution_time:.2f} seconds: {e}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise
    
    return wrapper

class ETLMonitor:
    """Monitor ETL pipeline health and send alerts"""
    
    def __init__(self, alert_email=None, smtp_server=None):
        self.alert_email = alert_email
        self.smtp_server = smtp_server
        self.logger = logging.getLogger(__name__)
    
    def check_data_freshness(self, last_update, max_age_hours=24):
        """Check if data is fresh enough"""
        if not last_update:
            raise DataQualityError("No last update timestamp found")
        
        age_hours = (datetime.now() - last_update).total_seconds() / 3600
        
        if age_hours > max_age_hours:
            error_msg = f"Data is {age_hours:.1f} hours old (max: {max_age_hours})"
            self.send_alert("Data Freshness Alert", error_msg)
            raise DataQualityError(error_msg)
    
    def check_record_counts(self, current_count, expected_min=0, expected_max=None):
        """Validate record counts are within expected ranges"""
        if current_count < expected_min:
            error_msg = f"Record count {current_count} below minimum {expected_min}"
            raise DataQualityError(error_msg)
        
        if expected_max and current_count > expected_max:
            error_msg = f"Record count {current_count} above maximum {expected_max}"
            raise DataQualityError(error_msg)
    
    def send_alert(self, subject, message):
        """Send email alert if configured"""
        if not self.alert_email or not self.smtp_server:
            self.logger.warning(f"Alert: {subject} - {message}")
            return
        
        try:
            msg = MIMEText(message)
            msg['Subject'] = f"ETL Pipeline Alert: {subject}"
            msg['From'] = 'etl-pipeline@company.com'
            msg['To'] = self.alert_email
            
            # This would need actual SMTP configuration
            self.logger.info(f"Alert sent: {subject}")
            
        except Exception as e:
            self.logger.error(f"Failed to send alert: {e}")

def validate_data_integrity(df, required_columns, unique_columns=None):
    """Comprehensive data integrity validation"""
    errors = []
    
    # Check required columns exist
    missing_cols = set(required_columns) - set(df.columns)
    if missing_cols:
        errors.append(f"Missing required columns: {missing_cols}")
    
    # Check for completely empty dataframe
    if df.empty:
        errors.append("Dataset is empty")
        return errors
    
    # Check unique constraints
    if unique_columns:
        for col in unique_columns:
            if col in df.columns:
                duplicates = df[col].duplicated().sum()
                if duplicates > 0:
                    errors.append(f"Found {duplicates} duplicate values in {col}")
    
    # Check for excessive null values
    for col in required_columns:
        if col in df.columns:
            null_pct = df[col].isnull().sum() / len(df) * 100
            if null_pct > 10:  # More than 10% null
                errors.append(f"Column {col} has {null_pct:.1f}% null values")
    
    return errors

if __name__ == "__main__":
    # Test error handling setup
    logger = setup_logging()
    logger.info("Error handling module ready")
    
    monitor = ETLMonitor()
    logger.info("ETL monitoring initialized")