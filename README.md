## Download NAV data from AMFI website and Store in local SQLite database


1. Run the script `download-nav.sh` to download data from AMFI website. This script downloads a .txt file for every month starting April 2006
1. Then run `python3 amfi.py`. This will create a sqlite file mfdb.sqlite3
1. The sqlite file should have approximately 26M records

## Database Table Schema
These Tables are automatically created. The script is kept here so that you can understand the schema

```
--- This table has 1 record per mutual fund per day
--- This should have about 26M records
CREATE TABLE IF NOT EXISTS nav_history(
    code varchar(8), 
    nav_date date, 
    nav integer, 
    repurchase_price integer, 
    sale_price integer
);

--- This is the master data for a mutual fund
--- code is the primary key per AMFI database
--- amc_id is foreign key to Amc table. Amc is Asset Management Company aka fund house
--- category_id is foreign key to fund_categories table
CREATE TABLE IF NOT EXISTS mutual_funds(
    code varchar(8) PRIMARY KEY,
    amc_id varchar(100),
    category_id varchar(100),
    name varchar(200),
    isin_growth varchar(20),
    isin_dividend_payout varchar(20),
    isin_dividend_reinvestment varchar(20)
);

--- Amc = Asset Management Company
--- Exampe: HDFC Mutual Fund
CREATE TABLE IF NOT EXISTS amc(
    id INTEGER PRIMARY KEY,
    name varchar(20)
);

--- This is the fund category as defined by AMFI
--- Example: Open Ended Schemes ( Money Market )
CREATE TABLE IF NOT EXISTS fund_categories(
    id INTEGER PRIMARY KEY,
    category varchar(20)
);
```
