from collections import namedtuple
from datetime import datetime
import os
import fnmatch
import sqlite3
from itertools import islice

CREATE_NAV_HISTORY_TABLE = """
    CREATE TABLE IF NOT EXISTS nav_history(
        code varchar(8), 
        nav_date date, 
        nav integer, 
        repurchase_price integer, 
        sale_price integer
    );
"""
INSERT_NAV_HISTORY_TABLE = """
    INSERT INTO nav_history(code, nav_date, nav, repurchase_price, sale_price)
    VALUES (?, ?, ?, ?, ?);
"""

CREATE_MUTUAL_FUND_TABLE = """
    CREATE TABLE IF NOT EXISTS mutual_funds(
        code varchar(8) PRIMARY KEY,
        amc_id varchar(100),
        category_id varchar(100),
        name varchar(200),
        isin_growth varchar(20),
        isin_dividend_payout varchar(20),
        isin_dividend_reinvestment varchar(20)
    );
"""

INSERT_MUTUAL_FUND_TABLE = """
    INSERT INTO mutual_funds(code, amc_id, category_id, name, 
    isin_growth, isin_dividend_payout, isin_dividend_reinvestment)
    VALUES (?, ?, ?, ?, ?, ?, ?);
"""

CREATE_AMC_TABLE = """
    CREATE TABLE IF NOT EXISTS amc(
        id INTEGER PRIMARY KEY,
        name varchar(20)
    );
"""

INSERT_AMC_TABLE = """
    INSERT INTO amc(id, name)
    VALUES (?, ?);
"""


CREATE_FUND_CATEGORIES_TABLE = """
    CREATE TABLE IF NOT EXISTS fund_categories(
        id INTEGER PRIMARY KEY,
        category varchar(20)
    );
"""

INSERT_FUND_CATEGORIES_TABLE = """
    INSERT INTO fund_categories(id, category)
    VALUES (?, ?);
"""


NavRecord = namedtuple('NavRecord', ['code', 'fund', 'isin1', 'isin2', 'nav', 'repurchase_price', 'sale_price', 'date'])
MutualFund = namedtuple('MutualFund', ['code', 'amc_id', 'category_id', 'name', 'isin_growth', 'isin_dividend_payout', 'isin_dividend_reinvestment'])
Amc = namedtuple('Amc', ['id', 'name'])
Category = namedtuple('Category', ['id', 'category'])

def chunker(iterable, n):
    i = iter(iterable)
    piece = list(islice(i, n))
    while piece:
        yield piece
        piece = list(islice(i, n))

def _list_files(root):
    # Find *.txt AMFI NAV files in the root directory
    # Sort the file names so that oldest files come up first, latest ones come up last
    for path, dirlist, filelist in os.walk(root):
        files = fnmatch.filter(filelist, "nav*.txt")
        files.sort()
        for f in files:
            yield os.path.join(path, f)

def _gen_open(filenames):
    # Returns a generator of open file objects
    for name in filenames:
        if name.endswith(".gz"):
            yield gzip.open(name)
        elif name.endswith(".bz2"):
            yield bz2.BZ2File(name)
        else:
            yield open(name)

def gen_combined_nav_lines(directory):
    # Combines all NAV files in the directory 
    # into a single, sorted stream of lines
    filenames = _list_files(directory)
    sources = _gen_open(filenames)
    for s in sources:
        for item in s:
            yield item

class AMFIParser:
    def __init__(self):
        self.mutual_funds = {}
        self.amc_by_name = {}
        self.amc_id_seq = 0
        self.category_by_name = {}
        self.category_id_seq = 0

    def parse(self, stream):
        current_amc_id = None
        current_category_id = None
        
        for line in stream:
            if self.is_record_line(line):
                record = self.to_record(line)
                mf = self.extract_mf_details(record, current_amc_id, current_category_id)
                self.mutual_funds[mf.code] = mf
                
                yield record
            elif self.is_header_line(line):
                continue
            elif self.is_blank_line(line):
                continue
            elif 'Schemes' in line:
                category = line.strip()
                if category in self.category_by_name:
                    current_category_id = self.category_by_name[category].id
                else:
                    self.category_id_seq += 1
                    current_category_id = self.category_id_seq
                    self.category_by_name[category] = Category(current_category_id, category)
            else:
                amc = line.strip()
                if amc in self.amc_by_name:
                    current_amc_id = self.amc_by_name[amc].id
                else:
                    self.amc_id_seq += 1
                    current_amc_id = self.amc_id_seq
                    self.amc_by_name[amc] = Amc(current_amc_id, amc)

    def extract_mf_details(self, record, amc_id, category_id):
        return MutualFund(record.code, amc_id, category_id, record.fund,
                record.isin1, record.isin1, record.isin2)

    def is_blank_line(self, line):
        line = line.strip()
        if not line:
            return True
        return False

    def is_header_line(self, line):
        return 'Scheme Code;Scheme Name' in line
    
    def is_record_line(self, line):
        if self.is_header_line(line):
            return False
        if ';' in line:
            return True
        return False

    def to_record(self, line):
        tokens = line.split(";")
        # isin1 and isin2 should be None, not empty
        for i in (0, 1):
            if not tokens[i+2]:
                tokens[i+2] = None

        # Convert NAV, Repurchase Price and Sale Price to integer
        for i in (0, 1, 2):
            tokens[i+4] = self.to_integer(tokens[i+4])
        
        # Convert date to an object
        tokens[7] = self.to_date(tokens[7])
        record = NavRecord(*tokens)
        return record

    def to_date(self, val):
        val = val.strip()
        return datetime.strptime(val, '%d-%b-%Y').date()

    def to_integer(self, val, raise_exception=False):
        # Convert string to long
        # Essentially, this is multiplying NAV by 10^4
        # NAV is like money, so we have to be careful not to use floats/doubles
        # Therefore, we multiply NAV by 10^4 so that we can use integers 
        try:
            val = val.replace(",", "")
            indexOfDecimal = val.find('.')
            val = val.replace(".", "")

            if indexOfDecimal == -1:
                zeroesToPad = 4
            else:
                zeroesToPad = 4 - len(val) + indexOfDecimal
            if zeroesToPad:
                val = val + "0" * zeroesToPad
            return int(val)
        except ValueError as e:
            if raise_exception:
                raise ValueError('Cannot convert ' + val + " to long") from  e
            else:
                return None

if __name__ == '__main__':
    conn = sqlite3.connect("mfdb.sqlite3")
    cur = conn.cursor()
    cur.execute(CREATE_NAV_HISTORY_TABLE)
    cur.execute(CREATE_MUTUAL_FUND_TABLE)
    cur.execute(CREATE_AMC_TABLE)
    cur.execute(CREATE_FUND_CATEGORIES_TABLE)

    combined_lines = gen_combined_nav_lines("data/")
    parser = AMFIParser()
    records = parser.parse(combined_lines)
    record_chunks = chunker(records, 10000)
    record_chunks = islice(record_chunks, 2)
    
    for chunk in record_chunks:
        cur.execute('BEGIN TRANSACTION')
        for record in chunk:
            cur.execute(INSERT_NAV_HISTORY_TABLE, (record.code, record.date, record.nav, record.repurchase_price, record.sale_price))
        cur.execute('COMMIT')
    
    cur.execute('BEGIN TRANSACTION')
    for code, mf in parser.mutual_funds.items():
        cur.execute(INSERT_MUTUAL_FUND_TABLE, 
            (mf.code, mf.amc_id, mf.category_id, mf.name,
            mf.isin_growth, mf.isin_dividend_payout, 
            mf.isin_dividend_reinvestment)
        )
    cur.execute('COMMIT')
    

    cur.execute('BEGIN TRANSACTION')
    for _, amc in parser.amc_by_name.items():
        cur.execute(INSERT_AMC_TABLE, (amc.id, amc.name))
    cur.execute('COMMIT')
    
    cur.execute('BEGIN TRANSACTION')
    for _, category in parser.category_by_name.items():
        cur.execute(INSERT_FUND_CATEGORIES_TABLE, (category.id, category.category))
    cur.execute('COMMIT')
    