import price_data
import datetime
import logging as log

log.basicConfig(filename='./logs/{}.price_data.log'.format(datetime.date.today()),
                format='%(asctime)s,%(msecs)03d...%(levelname)s:  %(message)s',
                datefmt='%H:%M:%S',
                level='DEBUG')

log.info('Executing update_price_data.py')

if price_data.dl_price_data():
    try:
        filename = price_data.get_latest_filename()
        with open(filename, 'r') as f:
            price_data.populate_table(file_obj=f)
    except:
        log.exception('Exception in updating price data:')
        raise
    else:
        log.info('Updating of price data complete.')