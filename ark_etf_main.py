import cathie

# DICTIONARY/LIST - a dictionary of keys (ARK ETF tickers) and values (ARK ETF url link); list of ARK ETF tickers
tickers_urls = cathie.tickers_list_urls_dictionary()
# WEB SCRAPE - download the ARK ETF csv files from the ARK website
cathie.Grab_files_from_internet(tickers_urls[1]).get_csv()
# GRAB DATE - look into each csv file, grab the date, and rename the file accordingly
today_date_dict = cathie.Grab_files_from_internet(tickers_urls[1]).get_date_rename_file()
# Create the summary file object to be used in later modules
summary_file = cathie.summary_file_name(today_date_dict)
# TRUNCATE CSV - remove the last three rows of each csv file
cathie.Edit_downloaded_files().remove_last_three_rows()
# PRICE AND RANK - add two columns to each csv file - price & rank
cathie.Edit_downloaded_files().calc_stock_price_and_rank()
# CREATE FOLDER TREE - create a folder structure if not already created
cathie.Folders_organize(tickers_urls[0]).create_etf_directories()
# CREATE SUB FOLDER TREE - create a sub directory folder structure if not already created
cathie.Folders_organize(tickers_urls[0]).create_etf_sub_directories()
# MOVE FILES - move each csv file to the appropriate ARK ETF specific folder
cathie.Folders_organize(tickers_urls[0]).move_today_files(today_date_dict)
# ARCHIVE - move each csv file to the appropriate archive ARK ETF specific folder
    # This method is not currently being used
    #cathie.Folders_organize(tickers_urls[0]).archive_today_files(today_date_dict)
# TODAY FILE - get the latest file from the archive folder
today_filepath_list = cathie.Get_working_files(tickers_urls[0]).today_files()
# YESTERDAY FILE - get the second latest file from the archive folder
yesterday_filepath_list = cathie.Get_working_files(tickers_urls[0]).yesterday_files(today_filepath_list)
# Create a data frame for each ARK ETF fund, both TODAY and YESTERDAY files to compare them
delta_filepath_list = cathie.ark_data_frames(tickers_urls[0], today_filepath_list, yesterday_filepath_list,
                                           today_date_dict)
# Calculate the total market value of each fund and the change from yesterday
fund_mv_list = cathie.fund_sum_market_value(tickers_urls[0], today_filepath_list, yesterday_filepath_list, summary_file)
# Notify the user if a stock has been added or removed from the etf fund
cathie.stocks_added_or_removed(tickers_urls[0], today_filepath_list, yesterday_filepath_list, summary_file)
# Find the median buy or sold % for each fund, and count how many stocks had shares sold at the median and mode
mode_median_message_list = cathie.median_mode_change_in_shares(delta_filepath_list)
# Notify the user if a stock has changed by +-5% of shares owned, share price, of change in rank by 5 or more positions
cathie.changed_x_or_more(tickers_urls[0], fund_mv_list[0], fund_mv_list[1], summary_file, mode_median_message_list)
# No longer needed, but will keep for future
#cathie.remove_duplicate_lines()

print('¡Hecho! ¡La programación esta terminada!')

