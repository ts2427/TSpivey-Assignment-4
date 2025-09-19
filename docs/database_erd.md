\# Database Entity-Relationship Diagram



\## Entities and Relationships



\### Companies

\- \*\*Primary Key\*\*: company\_id

\- \*\*Attributes\*\*: ticker, company\_name, sector, governance\_score

\- \*\*Relationships\*\*: One-to-many with stock\_prices, sec\_filings, cybersecurity\_incidents



\### Stock\_Prices

\- \*\*Primary Key\*\*: price\_id

\- \*\*Foreign Key\*\*: company\_id → companies.company\_id

\- \*\*Attributes\*\*: date, closing\_price, trading\_volume, returns

\- \*\*Relationships\*\*: Many-to-one with companies



\### SEC\_Filings

\- \*\*Primary Key\*\*: filing\_id

\- \*\*Foreign Key\*\*: company\_id → companies.company\_id

\- \*\*Attributes\*\*: filing\_date, filing\_type, cybersecurity\_mention, disclosure\_speed

\- \*\*Relationships\*\*: Many-to-one with companies



\### Cybersecurity\_Incidents

\- \*\*Primary Key\*\*: incident\_id

\- \*\*Foreign Key\*\*: company\_id → companies.company\_id

\- \*\*Attributes\*\*: breach\_date, disclosure\_date, incident\_type, records\_affected

\- \*\*Relationships\*\*: Many-to-one with companies



\## Key Indexes

\- stock\_prices(company\_id, date) - for time series queries

\- sec\_filings(filing\_date) - for event studies

\- companies(ticker) - for lookups

