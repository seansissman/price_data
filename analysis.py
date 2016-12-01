import price_data
import psycopg2
import pandas as pd
import datetime
import operator


def get_df(columns='*', tickers=(), startdate='', enddate=''):
    """ Returns the date of the last table update. """
    con = None
    qfilter = ''

    if tickers:
        qfilter = ' WHERE ticker in {}'.format(tickers)
    if startdate:
        f = ' date >= \'{}\'::DATE'.format(startdate)
        if qfilter:
            qfilter = qfilter + ' AND {}'.format(str(f))
        else:
            qfilter = ' WHERE {}'.format(f)
    if enddate:
        f = ' date <= \'{}\'::DATE'.format(enddate)
        if qfilter:
            qfilter = qfilter + ' AND {}'.format(f)
        else:
            qfilter = ' WHERE {}'.format(f)

    sql = 'SELECT {} FROM price_data{}'.format(columns, qfilter)
    print sql
    try:
        con = psycopg2.connect("dbname='tradingdb' user='trader' "
                               "host='localhost' password='123456'")
        return pd.read_sql(sql, con)
    except psycopg2.OperationalError, e:
        print "Unable to connect to the database:\n" + str(e)
        return None
    finally:
        if con:
            print "closing"
            con.close()


def sma(df, period):
    return pd.rolling_mean(df, window=period)


def rsi(df, period=210):
    if len(df) >= period:
        ma = sma(df, period)
        return df['adj_close'] / ma['adj_close']
    else:
        return None
    return rsi_ratio


def rsi_ratios():
    sdate = datetime.date.today() - datetime.timedelta(days=366)
    all_tickers = price_data.get_current_tickers()
    ratios = pd.Series(None, index=all_tickers)
    all_tickers_df = get_df(columns='date, ticker, adj_close', startdate=sdate)
    sorted_df = all_tickers_df.set_index(['ticker', 'date']).sort_index(0)

    for ticker in all_tickers:
        df = sorted_df.loc[ticker]
        df['sma'] = sma(df, 210)#.adj_close
        df['rsi'] = rsi(df)
        ratios[ticker] = df.iloc[-1].rsi

    ratios = ratios[ratios.notnull()]
    ratios.sort_values(inplace=True)
    print ratios.head()
    print ratios.tail(20)

rsi_ratios()


# df = get_df(columns='date, adj_close',
#             tickers=('AAPL',''),
#             startdate='2016-11-01',
#             enddate=datetime.date.today())
# print df.head()
# print df.tail()
#
# today = datetime.date.today()
# sdate = today - datetime.timedelta(days=366)
# all_tickers = price_data.get_all_tickers()
# df = get_df(columns='date, ticker, adj_close',
#             startdate=sdate)
# print df.tail()

def rsi_rank():
    sdate = datetime.date.today() - datetime.timedelta(days=366)
    all_tickers = price_data.get_current_tickers()
    rsi_ratios = {}
    # ranks = pd.DataFrame(columns='ratio', index='ticker')
    all_tickers_df = get_df(columns='date, ticker, adj_close', startdate=sdate)
    sorted_df = all_tickers_df.set_index(['ticker', 'date']).sort_index(0)

    for ticker in all_tickers:
        df = sorted_df.loc[ticker]
        # print df.tail()
        # df = sorted_df[sorted_df.ticker == ticker]
        df['sma'] = sma(df, 210).adj_close
        df['rsi'] = rsi(df)
        rsi_ratios[ticker] = df.iloc[-1].rsi
    rank_df = pd.DataFrame.from_dict(rsi_ratios, orient='index').columns('ratio')
    print rank_df.tail()
    rank = rank_df.rank(axis=0) / len(all_tickers) * 100
    print rank.tail()
    #     print rsi_ratios
    #     sorted_ratios = sorted(rsi_ratios.items(), key=operator.itemgetter(1))
    #     print sorted_ratios

    # print all_tickers_df.tail()
    # print sorted_df.tail()

# rsi_rank()
    # for ticker in all_tickers[]:#5:9]:
    #     print ticker
    #     df = get_df(tickers=(ticker, ''), columns='date, adj_close',
    #                 startdate=sdate)
    #     df['sma'] = sma(df, 210).adj_close.tail()
    #     print 'rsi-----------------'
    #     df['rsi'] = rsi(df)
    #     print df.tail()

    # print '444444444444444444'
    # print sma(df, 210).tail()

# columns = 'date, ticker, adj_close'
# my_df = get_df(tickers=('TSLA',''), columns=columns)
# my_df['rsi_ratio'] = rsi(my_df)
# print my_df.tail(30)


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

