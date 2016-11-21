import price_data

if price_data.dl_price_data():

    filename = price_data.get_latest_filename()
    print filename
    print 'inserting {} into db.'.format(filename)
    with open(filename, 'r') as f:
        price_data.populate_table(file_obj=f)

# price_data.get_all_tickers()
