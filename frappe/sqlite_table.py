# 데이터 저장에 필요한 table 만드는 스크립트

import sqlite3


def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file) # disk에 db file 만들어짐
        # conn = sqlite3.connect(':memory:') # memory(RAM)에 db file 만들어짐
        print(sqlite3.version)
    except sqlite3.Error as e:
        print(e)

    cursor = conn.cursor()
    bm_price_info = """
        CREATE TABLE BM_price (
            AsOfDate DATE,
            Symbol VARCHAR,
            Price DECIMAL,
            IndexName VARCHAR,
            PRIMARY KEY (AsOfDate, Symbol)
        );
    """

    trading = """
        CREATE TABLE Trading (
            AsOfDate DATE ,
            Symbol VARCHAR,
            CompanyCode VARCHAR,
            NAV DECIMAL,
            AUM DECIMAL,
            NetAssets DECIMAL,
            AdjustedNAV DECIMAL,
            ShareClassAUM DECIMAL,
            PRIMARY KEY(AsOfDate, Symbol)
        );
    """

    cursor.execute(bm_price_info)
    cursor.execute(trading)

    if conn:
        conn.close()


if __name__ == '__main__':
    db_file = "test.db"
    create_connection(db_file)
