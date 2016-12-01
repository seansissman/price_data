import psycopg2
import shutil
from urllib2 import urlopen  # Python 2
# from urllib.request import urlopen # Python 3
import datetime
import zipfile
import os
import glob
import logging
import myquandl as mq


# logging.basicConfig(filename='./logs/{}.log'.format(datetime.date.today()),
#                     format='%(asctime)s,%(msecs)03d...%(levelname)s:  %(message)s',
#                     datefmt='%H:%M:%S',
#                     level='DEBUG')
#
# logging.info('test')


def dbwrap(func):
    def sql_manager(*args, **kwargs):
        con = None
        retval = None
        try:
            con = psycopg2.connect("dbname='tradingdb' user='trader' "
                                   "host='localhost' password='123456'")
            cur = con.cursor()
            retval = func(cur, **kwargs)
            con.commit()
            return retval
        except psycopg2.OperationalError, e:
            print "Unable to connect to the database:\n" + str(e)

        finally:
            if con:
                print "closing"
                con.close()
    return sql_manager


@dbwrap
def populate_table(cur, **kwargs):
    """ Inserts CSV price data into price_data table. """
    file_obj = kwargs['file_obj']
    sql_copy = """
            COPY %s FROM STDIN WITH
            CSV
            HEADER
            DELIMITER AS ','
        """

    cur.execute("CREATE TABLE IF NOT EXISTS price_data(ticker VARCHAR(30), "
                "date DATE, open REAL, high REAL, low REAL, "
                "close REAL, volume REAL, ex_dividend REAL, "
                "split_ratio REAL, adj_open REAL, adj_high REAL, "
                "adj_low REAL, adj_close REAL, adj_volume REAL)")
    cur.copy_expert(sql=sql_copy % 'price_data', file=file_obj)
    print "insert complete."


@dbwrap
def latest_db_update(cur):
    """ Returns the datetime of the last table update. """
    cur.execute("SELECT date FROM price_data ORDER BY date DESC LIMIT 1")
    row = cur.fetchone()
    if row:
        # print "latest price"
        # print row[0].strftime("%Y-%m-%d")
        return row[0]
    else:
        return None


def get_quandl_url():
    """ Returns the URL to retrieve all data since last update. """
    ldate = latest_db_update()
    if ldate:
        if ldate < datetime.date.today():
            qurl = r'https://www.quandl.com/api/v3/datatables/WIKI/' \
                   r'PRICES.csv?date.gt={}&api_key={}'.format(ldate.strftime("%Y-%m-%d"),
                                                              mq.quandl_api_key())
        else:
            print "database is up to date"
            qurl = None
    else:
        qurl = r'https://www.quandl.com/api/v3/databases/WIKI/' \
               r'data?api_key={}'.format(mq.quandl_api_key())
    print qurl
    return qurl


def dl_price_data():
    """ Download price data from quandl needed to bring db up to date. """
    today = datetime.date.today()
    url = get_quandl_url()
    if url:
        if 'databases' in url:
            ext = 'zip'
        else:
            ext = 'csv'
    else:
        return False

    filename = '/home/sean/projects/price_env/price/data/' \
               '{}.{}'.format(today, ext)
    print filename
    print 'downloading file:  {}'.format(filename.split('/')[-1])
    req = urlopen(url)
    with open(filename, 'wb') as f:
        shutil.copyfileobj(req, f)

    if ext == 'zip':
        z = zipfile.ZipFile(filename)
        z.extractall()
        extracted_filename = z.namelist()[0]
        print 'extracting file:  {}'.format(extracted_filename)
        with open(extracted_filename, 'rb') as ef:
            saveas_filename = filename[:-3] + 'csv'
            print 'saving as:  {}'.format(saveas_filename)
            with open(saveas_filename, 'wb') as csv_filename:
                shutil.copyfileobj(ef, csv_filename)
    return True


def get_latest_filename():
    """ Returns the latest price history CSV file. """
    path = './data/'
    return max(glob.iglob('{}*.[Cc][Ss][Vv]'.format(path)), key=os.path.getctime)


@dbwrap
def get_all_tickers(cur):
    """ Returns list of all existing tickers in price_data table"""
    cur.execute("SELECT ticker FROM price_data GROUP BY ticker ORDER BY ticker")
    rows = cur.fetchall()
    tic = [t[0] for t in rows]
    return tic

@dbwrap
def get_current_tickers(cur):
    """ Returns list of all tickers from latest date"""
    date = latest_db_update()
    sql = "SELECT ticker FROM price_data WHERE date >= '{}'::DATE " \
          "GROUP BY ticker ORDER BY ticker".format(date)
    cur.execute(sql)
    rows = cur.fetchall()
    tic = [t[0] for t in rows]
    return tic

#get_all_tickers()