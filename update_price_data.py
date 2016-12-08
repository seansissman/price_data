from price_data import pdlog, \
                       dl_price_data, \
                       get_latest_filename, \
                       get_latest_filesize, \
                       populate_table
from sys import exit


pdlog.info('Executing update_price_data.py')

if dl_price_data():
    try:
        filename = get_latest_filename()
        if get_latest_filesize() > 110:
            with open(filename, 'r') as f:
                populate_table(file_obj=f)
        else:
            pdlog.warning('Latest CSV file contains no data.  Aborting update.')
            exit(1)
    except:
        pdlog.exception('Exception in updating price data:')
        raise
    else:
        pdlog.info('Updating of price data complete.')
