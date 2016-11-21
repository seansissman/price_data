import price_data
import psycopg2
import pandas as pd


def get_df(tickers=(), columns='*'):
    """ Returns the date of the last table update. """
    con = None
    if tickers:
        sql = 'SELECT {} FROM price_data where ticker in {}'.format(columns, tickers)
    else:
        sql = 'SELECT {} FROM price_data'.format(columns)
    print sql
    try:
        con = psycopg2.connect("dbname='tradingdb' user='trader' "
                               "host='localhost' password='123456'")
        return pd.read_sql(sql, con, chunksize=10000)
    except psycopg2.OperationalError, e:
        print "Unable to connect to the database:\n" + str(e)
        return None
    finally:
        if con:
            print "closing"
            con.close()


def sma(df, period):
    return pd.rolling_mean(df, window=period)


def rsi(df):
    sma210 = sma(df, 210)
    rsi210 = df['adj_close'] / sma210['adj_close']
    print rsi210


# columns = 'date, ticker, adj_close, adj_volume'
# my_df = get_df(tickers=('FB', 'TSLA'), columns=columns)
# # the_df = my_df.set_index('date')
#
# # rsi(the_df)
#
# df = get_df(columns=columns)
# print df[df['ticker'] == 'AAPL']

# df = get_df()
# print df.head()
# print my_df.head()
# print my_df.tail()

