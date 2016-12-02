import psycopg2
import shutil
from urllib2 import urlopen  # Python 2
# from urllib.request import urlopen # Python 3
import datetime
import zipfile
import os
import glob
import logging as log
import myquandl as mq


log.basicConfig(filename='./logs/{}.price_data.log'.format(datetime.date.today()),
                format='%(asctime)s,%(msecs)03d...%(levelname)s:  %(message)s',
                datefmt='%H:%M:%S',
                level='DEBUG')


def dbwrap(func):
    """ Wrapper for db connections. """
    def sql_manager(*args, **kwargs):
        con = None
        retval = None
        try:
            log.info('Connecting to db "tradingdb".')
            con = psycopg2.connect("dbname='tradingdb' user='trader' "
                                   "host='localhost' password='123456'")
            cur = con.cursor()
            retval = func(cur, **kwargs)
            con.commit()
            return retval
        except psycopg2.OperationalError:
            log.exception('Unable to connect to the db.')
            raise
        finally:
            if con:
                log.info("Closing db connection")
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
    log.info('Attempting db insert from file:\n\t{}'.format(file_obj.name))
    cur.copy_expert(sql=sql_copy % 'price_data', file=file_obj)
    log.info('Insert complete.')


@dbwrap
def latest_db_update(cur):
    """ Returns the datetime of the last table update. """
    cur.execute("SELECT date FROM price_data ORDER BY date DESC LIMIT 1")
    row = cur.fetchone()
    if row:
        log.info('latest_db_update() determined latest update is:\n\t{}'.format(row[0]))
        return row[0]
    else:
        log.info('latest_db_update() determined the table is empty.')
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
    log.info('get_quandl_url() determined the proper URL is:\n\t{}'.format(qurl))
    return qurl


def extract_from_zip(filename):
    """ Extracts and saves the a CSV file from a ZIP file. """
    z = zipfile.ZipFile(filename)
    z.extractall()
    extracted_filename = z.namelist()[0]
    log.info('Extracting file:  {}'.format(extracted_filename))
    with open(extracted_filename, 'rb') as ef:
        saveas_filename = filename[:-3] + 'csv'
        log.info('Saving as:  {}'.format(saveas_filename))
        with open(saveas_filename, 'wb') as csv_filename:
            shutil.copyfileobj(ef, csv_filename)
    log.info('Extracted file from zip archive:\n\t{}'.format(saveas_filename))


def dl_price_data():
    """ Download price data from quandl needed to bring db up to date. """
    try:
        today = datetime.date.today()
        url = get_quandl_url()
        log.info('Attempting to download price data from:\n\t{}'.format(url))
        if url:
            if 'databases' in url:
                ext = 'zip'
            else:
                ext = 'csv'
        else:
            log.error('Bad URL')
            return False

        filename = '/home/sean/projects/price_env/price/data/' \
                   '{}.{}'.format(today, ext)
        log.info('Saving file to:\n\t{}'.format(filename))
        req = urlopen(url)
        with open(filename, 'wb') as f:
            shutil.copyfileobj(req, f)

        if ext == 'zip':
            extract_from_zip(filename)
    except:
        log.exception('Exception downloading price data.')
        raise
    else:
        log.info('Download assumed successful.')
        return True


def get_latest_filename():
    """ Returns the latest price history CSV file. """
    path = './data/'
    latest_file = max(glob.iglob('{}*.[Cc][Ss][Vv]'.format(path)), key=os.path.getctime)
    log.info('Checking which is the latest price data file:\n\t{}'.format(latest_file))
    return latest_file


@dbwrap
def get_all_tickers(cur):
    """ Returns list of all existing tickers in price_data table"""
    sql = "SELECT ticker FROM price_data GROUP BY ticker ORDER BY ticker"
    log.info('Retrieving list of all tickers from db\n\t{}'.format(sql))
    cur.execute(sql)
    rows = cur.fetchall()
    return [t[0] for t in rows]


@dbwrap
def get_current_tickers(cur):
    """ Returns list of all tickers from latest date"""
    date = latest_db_update()
    sql = "SELECT ticker FROM price_data WHERE date >= '{}'::DATE " \
          "GROUP BY ticker ORDER BY ticker".format(date)
    log.info('Retrieving list of current tickers from db\n\t{}'.format(sql))
    cur.execute(sql)
    rows = cur.fetchall()
    return [t[0] for t in rows]
