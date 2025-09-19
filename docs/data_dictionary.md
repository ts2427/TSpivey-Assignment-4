\# Data Dictionary



\## Companies Table

| Column | Type | Description | Constraints |

|--------|------|-------------|-------------|

| company\_id | SERIAL | Primary key | NOT NULL |

| ticker | VARCHAR(10) | Stock ticker symbol | UNIQUE, NOT NULL |

| company\_name | VARCHAR(255) | Full company name | |

| sector | VARCHAR(100) | Industry sector | |

| governance\_score | DECIMAL(3,2) | Governance quality (0-10) | |



\## Stock\_Prices Table

| Column | Type | Description | Constraints |

|--------|------|-------------|-------------|

| price\_id | SERIAL | Primary key | NOT NULL |

| company\_id | INTEGER | Foreign key to companies | NOT NULL |

| date | DATE | Trading date | NOT NULL |

| closing\_price | DECIMAL(10,2) | End of day stock price | > 0 |

| trading\_volume | BIGINT | Number of shares traded | >= 0 |

| returns | DECIMAL(8,6) | Daily return calculation | |



\## SEC\_Filings Table

| Column | Type | Description | Constraints |

|--------|------|-------------|-------------|

| filing\_id | SERIAL | Primary key | NOT NULL |

| company\_id | INTEGER | Foreign key to companies | NOT NULL |

| filing\_date | DATE | Date of SEC filing | NOT NULL |

| filing\_type | VARCHAR(10) | Type (8-K, 10-K, etc.) | NOT NULL |

| cybersecurity\_mention | BOOLEAN | Contains cyber keywords | DEFAULT FALSE |

| disclosure\_speed | VARCHAR(20) | Immediate/Delayed | |



\## Cybersecurity\_Incidents Table

| Column | Type | Description | Constraints |

|--------|------|-------------|-------------|

| incident\_id | SERIAL | Primary key | NOT NULL |

| company\_id | INTEGER | Foreign key to companies | NOT NULL |

| breach\_date | DATE | Date breach occurred | NOT NULL |

| disclosure\_date | DATE | Date company disclosed | |

| incident\_type | VARCHAR(50) | Type of breach | |

| records\_affected | INTEGER | Number of records breached | >= 0 |

