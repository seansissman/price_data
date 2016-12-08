from price_data import get_current_tickers
import psycopg2
import pandas as pd
import datetime
import logging
import emailer


alog = logging.getLogger('analysis_logger')
aformatter = logging.Formatter('%(asctime)s,%(msecs)03d...%(levelname)s:  %(message)s')
ahandler = logging.FileHandler('./logs/{}.analysis.log'.format(datetime.date.today()))
ahandler.setFormatter(aformatter)
alog.addHandler(ahandler)
alog.setLevel(logging.DEBUG)

aemail = emailer.Emailer(body='Price Data Analysis for {}'.format(datetime.date.today()))

def sql_filters(columns, tickers, startdate, enddate):
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
    return 'SELECT {} FROM price_data{}'.format(columns, qfilter)


def get_df(columns='*', tickers=(), startdate='', enddate=''):
    """ Returns the date of the last table update. """
    con = None
    if columns != '*' or tickers or startdate or enddate:
        sql = sql_filters(columns, tickers, startdate, enddate)
    else:
        sql = 'SELECT * FROM price_data'

    try:
        con = psycopg2.connect("dbname='tradingdb' user='trader' "
                               "host='localhost' password='123456'")
        return pd.read_sql(sql, con)
    except psycopg2.OperationalError, e:
        print "Unable to connect to the database:\n" + str(e)
        return None
    finally:
        if con:
            con.close()


def sma(df, period):
    """ Returns simple moving average"""
    # return pd.rolling_mean(df, window=period)
    return df.rolling(window=period, center=False).mean()


def rsi(df, period=210):
    if len(df) >= period:
        ma = sma(df, period)
        return df['adj_close'] / ma['adj_close']
    else:
        return None
    return rsi_ratio


def rsi_ratios(period=427, buy_ratio=1.47, sell_ratio=1.12):
    """ Calculates and filters RSI ratios. """
    alog.info('Calculating RSI ratios with the following criteria:\n'
             '\tPeriod = {}\n'
             '\tBuy Ratio = {}'.format(period, buy_ratio))
    sdate = datetime.date.today() - datetime.timedelta(days=period*2)
    all_tickers = get_current_tickers()
    ratios = pd.DataFrame(None, columns=['yesterday', 'today'], index=all_tickers)
    all_tickers_df = get_df(columns='date, ticker, adj_close', startdate=sdate)
    sorted_df = all_tickers_df.set_index(['ticker', 'date']).sort_index(0)

    for ticker in all_tickers:
        df = sorted_df.loc[ticker]
        df['sma'] = sma(df, period)#.adj_close
        df['rsi'] = rsi(df, period=period)
        if len(df) > 1:
            ratios.loc[ticker] = [df.iloc[-2].rsi, df.iloc[-1].rsi]

        ratios = ratios[ratios.notnull()]

    to_buy = ratios[(ratios.today >= buy_ratio) & (ratios.today < buy_ratio + 0.01) &
                    (ratios.yesterday < ratios.today)]
    # Attempt to format the dataframe so it looks nice in email - didn't work...
    # to_buy = buy_ratios.to_string(justify='left', col_space=8, index_spaces)

    alog.info('RSI calculations complete.  Resulting '
              'tickers within buy range:\n{}'.format(to_buy))
    print to_buy
    aemail.append_body('RSI Analysis with the following parameters:\n'
                       '\tLookback Period:\t{}\n'
                       '\tBuy Ratio:\t{}\n'
                       '\tTickers that rose into the buy ratio:\n{}'.format(period,
                                                                              buy_ratio,
                                                                              to_buy))

rsi_ratios(period=427, buy_ratio=1.47)
aemail.send_email()

#  INCOMPLETE
def rsi_rank():
    sdate = datetime.date.today() - datetime.timedelta(days=366)
    all_tickers = get_current_tickers()
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

