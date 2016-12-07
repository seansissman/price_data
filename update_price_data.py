from price_data import log, \
                       dl_price_data, \
                       get_latest_filename, \
                       get_latest_filesize, \
                       populate_table
from sys import exit


log.info('Executing update_price_data.py')

if dl_price_data():
    try:
        filename = get_latest_filename()
        if get_latest_filesize() > 110:
            with open(filename, 'r') as f:
                populate_table(file_obj=f)
        else:
            log.warning('Latest CSV file contains no data.  Aborting update.')
            exit(1)
    except:
        log.exception('Exception in updating price data:')
        raise
    else:
        log.info('Updating of price data complete.')
# else:
#     log.info('No update needed')