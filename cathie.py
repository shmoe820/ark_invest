import os
import csv
import time
import datetime
import shutil
import pandas as pd

def tickers_list_urls_dictionary():
    # Call on 'ark_funds.txt' file and create a list of tickers and a dict of keys (ticker) and values (website)
    tickers = []
    urls = {}
    with open("ark_funds.txt") as file:
        for line in file:
            key, value = line.split()
            urls[key] = value
            tickers.append(key)
    return tickers, urls

class Grab_files_from_internet():
    """Get csv files form ARK website, and do initial formatting of the files"""
    def __init__(self, urls):
        self.urls = urls

    def get_csv(self):
        # Download ARK ETF files from the Ark-Invest website
        for ticker, url in self.urls.items():
            print("PROCESSING {}".format(ticker))

            ticker_filename = ticker + ".csv"
            dl_folder = os.path.join(os.getcwd(), ticker_filename)

            dl_cmd = 'curl -o {} {}'.format(dl_folder, url)
            os.system(dl_cmd)

    def get_date_rename_file(self):
        """Grab date from within the file for proper date labeling and rename the file"""
        # empty dictionary that will contain 'ticker':'date' key, value pairs
        self.file_date_dict = {}
        # grab the date from each file
        for ticker, urls in self.urls.items():
            with open(ticker + '.csv', 'r+') as csvFile:
                next(csv.reader(csvFile))
                # grabs second row, first item which is the date in a DD/MM/YYYY format
                date_published = next(csv.reader(csvFile))[0]
            date_published = datetime.datetime.strptime(date_published, "%m/%d/%Y")
            # arrange the date to YYYY_MM_DD format (ensures proper order when sorting)
            date_published = datetime.datetime.strftime(date_published, "%Y_%m_%d")
            # populate the dictionary
            self.file_date_dict[ticker] = date_published
            # revise file name to reflect the new date
            new_file_name = ticker + '_' + date_published + '.csv'
            shutil.move(ticker + '.csv', new_file_name)
        return self.file_date_dict

def summary_file_name(today_date_dict):
    """Create the summary filename"""
    summary_file = 'summary\\' + 'summary_' + today_date_dict['ARKK'] + '.txt'
    return summary_file

class Edit_downloaded_files():
    """Perform additional formatting on the csv files, including removing the last 3 rows of each file and
    calculating some initial metrics"""

    def remove_last_three_rows(self):
        """remove the last 3 lines from all files in working directory that end in '.csv' """
        for file in os.listdir():
            if file.endswith('.csv'):
                with open(file, 'r+') as csvFile:
                    # remove the last 3 lines of each .csv file
                    lines = csvFile.readlines()
                    lines.pop()
                    lines.pop()
                    lines.pop()
                # clear out the file, and copy in the file with 3 rows removed
                with open(file, 'w+') as csvFile:
                    csvFile.writelines(lines)

    def calc_stock_price_and_rank(self):
        """add stock price and rank to all files in working directory that end in '.csv' """
        for file in os.listdir():
            if file.endswith('.csv'):
                with open(file, 'r+'):
                    df = pd.read_csv(file)
                    df["share price"] = df["market value($)"] / df["shares"]
                    df['share price'] = df['share price'].round(decimals=2)
                    # rank is a reflection of the %wt that each stock has in the etf
                    # Each stock has been given a rank that is 3 digits to allow for easier sorting
                    # This avoids sorting that results in [1,10,11,12....19,2,20,21,22...29,3,30,31]
                    df['rank'] = df.index + 101
                    df.to_csv(file, index=False)

class Folders_organize():
    """Move files to created/existing folder tree"""
    def __init__(self, ticker):
        self.ticker = ticker

    def create_etf_directories(self):
        """ setup directory for each fund, using the ticker symbols as the folder name"""
        for fund in self.ticker:
            fund_dir = os.path.join(os.getcwd(), fund)
            if not os.path.exists(fund_dir):
                os.mkdir(fund_dir)
        # setup summary directory to place summary files
        fund_dir = os.path.join(os.getcwd(), 'summary')
        if not os.path.exists(fund_dir):
            os.mkdir(fund_dir)

    def create_etf_sub_directories(self):
        """ setup sub-directories for each fund, archive and delta"""
        for fund in self.ticker:
            # setup archive directory
            archive_dir = os.path.join(os.getcwd(), fund, "archive")
            if not os.path.exists(archive_dir):
                os.mkdir(archive_dir)

            # setup delta dir; delta is synonymous with change; this is where the comparison files will be stored
            delta_dir = os.path.join(os.getcwd(), fund, "delta")
            if not os.path.exists(delta_dir):
                os.mkdir(delta_dir)

    def move_today_files(self, today_date_dict):
        """ move files to the appropriate folders"""
        for ticker in self.ticker:
            fund_dir = os.path.join(os.getcwd(), ticker)
            file_name = ticker + '_' + today_date_dict[ticker] + '.csv'
            # if this file path does not exist, move the file, otherwise delete
            if not os.path.isfile(fund_dir + '\\' + 'archive' + '\\' + file_name):
                shutil.move(os.getcwd() + '\\' + file_name, fund_dir + '\\' + 'archive')
            else:
                os.unlink(file_name)

    def archive_today_files(self, today_date_dict):
        """take the latest single file in the fund_dir and move it to the archive"""
        # this method is not being used; downloaded files move via method 'cathie.Folders_organize.move_today_files'
        for fund in self.ticker:
            # if this file path does not exist, move the file, otherwise delete
            if not os.path.isfile(
                    os.getcwd() + '\\' + fund + '\\' + "archive" + '\\' + fund + '_' + today_date_dict[fund] + '.csv'):
                shutil.move(os.getcwd() + '\\' + fund + '\\' + fund + '_' + today_date_dict[fund] + '.csv',
                            os.getcwd() + '\\' + fund + '\\' + "archive")
            else:
                os.unlink(os.getcwd() + '\\' + fund + '\\' + fund + '_' + today_date_dict[fund] + '.csv')

class Get_working_files():
    """Create a list of the files that will be compared to one another"""
    def __init__(self, ticker):
        self.ticker = ticker

    def today_files(self):
        """ get the filepaths for each files downloaded today"""
        today_filepath_list = []
        for fund in self.ticker:
            # grab the latest file that is in the archive folder
            directory = os.path.join(os.getcwd(), fund, 'archive')
            all_csv_files = [os.path.join(directory, x) for x in os.listdir(directory) if fund in x]
            all_csv_files = sorted(all_csv_files, reverse=True)
            # thanks to the date formatting, a reverse sort of the .csv files in the folder means the latest file is
            # in position [0]
            tday_file = all_csv_files[0]
            today_filepath_list.append(tday_file)
        return today_filepath_list

    def yesterday_files(self, today_filepath_list):
        yesterday_filepath_list = []
        x=0
        for fund in self.ticker:
            # grab the latest file that is in the archive folder
            directory = os.path.join(os.getcwd(), fund, 'archive')
            all_csv_files = [os.path.join(directory, x) for x in os.listdir(directory) if fund in x]
            all_csv_files = sorted(all_csv_files, reverse=True)
            # thanks to the date formatting YYYY_MM_DD, a reverse sort of the .csv files in the folder means the latest
            # file is in position [1]. if there is no file in position [1], the program will make a copy of the file to
            # allow the program to continue without error, and label the file with a date 1000 years ago
            try:
                yday_file = all_csv_files[1]
            except IndexError:
                print(fund + " fund is missing a 2nd file to compare today's file.  A copy with year dated 1021 was "
                             "created.")
                yday_file = today_filepath_list[x]
                yday_file = list(yday_file)
                yday_file[-14] = '1'
                yday_file = ''.join(yday_file)
                shutil.copy(today_filepath_list[x], os.path.join(os.getcwd(), fund, 'archive', yday_file))
            x += 1
            yesterday_filepath_list.append(yday_file)
        return yesterday_filepath_list

def ark_data_frames(ticker, today_filepath_list, yesterday_filepath_list, today_date_dict):
    """The meat of the program - turn today's file and yesterday's files to data frames and compare holdings"""
    y=0
    # Not used until the end of this method, creates a list of filepaths for the delta files
    delta_filepath_list = []
    for fund in ticker:
        # create a data frame for both today and yesterdays files
        t_df = pd.read_csv(today_filepath_list[y])
        y_df = pd.read_csv(yesterday_filepath_list[y])

        # THIS NEXT PART NEEDS A MORE CONCISE EXPLANATION; I APOLOGIZE IF IT IS CONFUSING
        # The dictionaries below will match each stock's unique CUSIP identifier to the associated metric
        # (Ticker may be used instead of CUSIP, however, Morgan Stanley Cash has no ticker value in the csv files)

        # Shares Dictionary; keys: CUSIP; values: change in number of shares of a specific stock in the etf
        d_shares_dict = {}
        # Rank Dictionary; keys: CUSIP; values: change in overall position in the etf
        d_rank_dict = {}
        # Market Value Dictionary; keys: CUSIP; values: change in market value of a specific stock in the etf
        d_market_value_usd_dict = {}
        # Weight % Dictionary; keys: CUSIP; values: change in weight % a specific stock in the etf
        d_weight_pct_dict = {}
        # List of dictionaries listed above; the values in each dictionary will be output to a list that will be
        # added to the csv file. The way of populating the csv is likely not the easiest way to do this.
        dicts_sums = [d_shares_dict, d_rank_dict, d_market_value_usd_dict, d_weight_pct_dict]

        # empty lists that will eventually contain the values from the dictionaries above
        shares = []
        rank = []
        market_value = []
        weight_pct = []
        # List of lists that will be populated with the values from the dictionaries above
        lists_sums = [shares, rank, market_value, weight_pct]

        # For each new heading to be created in the csv file, a dictionary was created that will contain the stock's
        # CUSIP identifier and calculate the difference (i.e. delta, or change) from the day before.
        x = 0
        # heading_sums are the labels that already exist in the csv file that will be called by the following loop.
        # 'shares' from yesterday to today will be compared, the delta is entered into a dictionary, and then
        # formatted to a list to be added to the csv file
        headings_sums = ['shares', 'rank', 'market value($)', 'weight(%)']
        # this is where the csv files are compared, the deltas calculated, entered into a dictionary and then to a list
        for heading in headings_sums:
            for cusip in t_df['cusip'].unique()[:]:
                today = t_df[t_df['cusip'] == cusip][heading].values
                yday = y_df[y_df['cusip'] == cusip][heading].values
                diff = today - yday
                dicts_sums[x][cusip] = diff
            # the values in the dictionary's keys and values are appended to a list
            for key, value in dicts_sums[x].items():
                try:
                    lists_sums[x].append(value[0])
                except IndexError:
                    lists_sums[x].append('N/A')
            x += 1

        # The above calculation was a delta by subtraction; the below calculation is delta as a percentage
        # Shares % Dictionary; keys: CUSIP; values: change in number of shares of a specific stock in the etf as a %
        d_shares_pct_dict = {}
        # Weight % Dictionary; keys: CUSIP; values: change in weight % of a specific stock in the etf as a % (% of a %)
        d_weight_pct_pct_dict = {}
        # Share Price % Dictionary; keys: CUSIP; values: change in share price of a specific stock in the etf as a %
        d_share_price_pct_dict = {}
        # List of dictionaries listed above; the values in each dictionary will be output to a list that will be
        # added to the csv file. The way of populating the csv is likely not the easiest way to do this.
        dicts_pcts = [d_shares_pct_dict, d_weight_pct_pct_dict, d_share_price_pct_dict]

        # empty lists that will eventually contain the values from the dictionaries above
        shares_pct = []
        weight_pct_pct = []
        share_price_pct = []
        # List of lists that will be populated with the values from the dictionaries above
        lists_pcts = [shares_pct, weight_pct_pct, share_price_pct]
        # heading_pcts are the labels that already exist in the csv file that will be called by the following loop.
        # 'shares' from yesterday to today will be compared, the delta is entered into a dictionary and then
        # formatted to a list to be added to the csv file
        headings_pcts = ['shares', 'weight(%)', 'share price']

        # this is where the csv files are compared, the deltas calculated, entered into a dictionary and then to a list
        x = 0
        for heading in headings_pcts:
            for cusip in t_df['cusip'].unique()[:]:
                today = t_df[t_df['cusip'] == cusip][heading].values
                yday = y_df[y_df['cusip'] == cusip][heading].values
                diff = (100 * (today - yday) / yday).round(decimals=2)
                dicts_pcts[x][cusip] = diff
            # the values in the dictionary's keys and values are appended to a list
            for key, value in dicts_pcts[x].items():
                try:
                    lists_pcts[x].append(value[0])
                except IndexError:
                    lists_pcts[x].append('N/A')
            x += 1

        # a data frame for the files downloaded today are created; the headings in the csv are listed here
        headings = ['date', 'fund', 'company', 'ticker', 'cusip', 'shares', 'market value($)', 'weight(%)',
                    'share price', 'rank']
        # Create data frame for today's CSV files (df1)
        with open(today_filepath_list[y], 'r') as f:
            df1 = pd.read_csv(f)
            df1 = pd.DataFrame(df1, columns=headings)
        # define new columns to be added to today's csv files
        df1['d_rank'] = rank
        df1['d_shares'] = shares
        df1['d_shares_pct'] = shares_pct
        df1['d_market_value($)'] = market_value
        df1['d_weight(%)'] = weight_pct
        df1['d_weight_pct_pct'] = weight_pct_pct
        df1['d_share_price_pct'] = share_price_pct


        #rename the csv file to the 'delta' folder in the folder tree
        title = fund + '_' + today_date_dict[fund] + '_delta.csv'
        print('Saving ' + fund + ' file...')
        df1.to_csv(os.path.join(os.getcwd(), fund, "delta", title), index=False)


        # A lengthy way of adding a new column called yesterday's share price and change in market value
        headings = ['date', 'fund', 'company', 'ticker', 'cusip', 'shares', 'market value($)', 'weight(%)',
                    'share price', 'rank', 'd_rank', 'd_shares', 'd_shares_pct', 'd_market_value($)', 'd_weight(%)',
                    'd_weight_pct_pct', 'd_share_price_pct']
        # Populate delta_filepath_list with list of filepaths for delta files
        delta_filepath_list.append(os.path.join(os.getcwd(), fund, "delta", title))
        z = 0
        for file in delta_filepath_list:
            # Create data frame for today's CSV files (df1)
            with open(file, 'r') as f:
                df1 = pd.read_csv(f)
                df1 = pd.DataFrame(df1, columns=headings)
            df1['yesterday_share_price'] = df1['share price'] / (1 + df1['d_share_price_pct'] / 100)
            df1['yesterday_share_price'] = df1['yesterday_share_price'].round(decimals = 2)

            df1['d_market_value($)_pct'] = df1['d_market_value($)'] / (df1['market value($)'] - df1['d_market_value($)'])* 100
            df1['d_market_value($)_pct'] = df1['d_market_value($)_pct'].round(decimals=2)

            df1.to_csv((file), index=False)
            z += 1
        y += 1
    return delta_filepath_list


def fund_sum_market_value(ticker, today_filepath_list, yesterday_filepath_list, summary_file):
    """Go into each file and summarize the overall market value, the % change from yesterday"""
    x=0
    # Create empty lists that will contain the fund market value of yesterday and today, and the change for each etf

    fund_sum_today_list = []
    fund_sum_yesterday_list = []
    d_fund_market_value_pct_list = []
    # Create data frames for today's and yesterday's ARK CSV files
    for fund in ticker:
        with open(today_filepath_list[x], 'r') as f:
            df1 = pd.read_csv(f)
            df1 = pd.DataFrame(df1)
        with open(yesterday_filepath_list[x], 'r') as f:
            df2 = pd.read_csv(f)
            df2 = pd.DataFrame(df2)
        #print(df1['ticker'][0])
        fund_sum_today = int(df1['market value($)'].sum())
        # This does not use the 'int' function because it messes up the equation below (can't round a float??)
        fund_sum_yesterday = df2['market value($)'].sum()
        # Calculate the change in the market value of each fund
        d_fund_market_value_pct = ((fund_sum_today-fund_sum_yesterday)/fund_sum_yesterday*100).round(decimals=2)
        # Populate the empty lists created above with the % change in market value, today and yesterdays market value

        fund_sum_today_list.append(fund_sum_today)

        fund_sum_yesterday = int(fund_sum_yesterday)
        fund_sum_yesterday_list.append(fund_sum_yesterday)

        d_fund_market_value_pct_list.append(d_fund_market_value_pct)

        x+=1

    # Tally up the total market value of all ARK ETFs for Today files
    total_assets_today = 0
    for value in fund_sum_today_list:
        total_assets_today += value
    # Tally up the total market value of all ARK ETFs for Yesterday files
    total_assets_yesterday = 0
    for value in fund_sum_yesterday_list:
        total_assets_yesterday += value

    # Find the difference (subtraction) of the overall market value for all funds
    total_change_mv_all_funds = total_assets_today - total_assets_yesterday
    # Find the % change of the overall market value for all funds
    total_change_mv_all_funds_pct = str(((total_assets_today - total_assets_yesterday) /
        total_assets_yesterday * 100))


    # First message to be written in the summary file: overall ARK status
    #summary_file_name = 'summary\\' + 'summary_' + today_date_dict['ARKK'] + '.txt'

    with open(summary_file, 'w+') as file:
        file.write("Cathie's ARK: \n\t$" + \
                   str(f'{total_assets_yesterday:,}') + ' --> $' + str(f'{total_assets_today:,}') + \
                   ' \n\tChange: $' + str(f'{total_change_mv_all_funds:,}') + ' (' + \
                   total_change_mv_all_funds_pct[:5] + '%)\n\n')
    #The returns below will be used for the ONE LINE summary of each stock that is is triggered by a condition in the
    # method 'def changed_x_or_more'
    return fund_sum_today_list, d_fund_market_value_pct_list

def stocks_added_or_removed(ticker, today_filepath_list, yesterday_filepath_list, summary_file):
    """Add to summary text file for any tickers that were added or removed from any ARK etf"""
    y=0
    with open(summary_file, 'a+') as file:
        file.write('Did ARK add or remove any stocks from their funds?\n\n')
    # Loop through each ticker and compare the TODAY and YESTERDAY files
    for fund in ticker:
        # create a data frame for the today file and the yesterday file
        t_df = pd.read_csv(today_filepath_list[y])
        y_df = pd.read_csv(yesterday_filepath_list[y])

        # Print the ticker for the fund in the text file
        with open(summary_file, 'a+') as file:
            file.write(fund + ':\n')
        # Loop through a list of companies in each ARK etf from YESTERDAY.  If it is not in TODAY's list of
        # companies, then that means the company was removed from the ARK etf
        x=0
        for company in y_df['company']:
            if str(company) not in list(t_df['company']):
                removed = '{}{}{}{}{}{}'.format('\t',y_df['ticker'][x],': ', company,': removed from ',fund)
                with open(summary_file, 'a+') as file:
                    file.write(removed)
                    file.write('\n')
            x+=1
        # Loop through a list of companies in each ARK etf from TODAY.  If it is not in YESTERDAY's list of
        # companies, then that means the company was added from the ARK etf
        x=0
        for company in t_df['company']:
            if str(company) not in list(y_df['company']):
                added = '{}{}{}{}{}{}{}'.format('\t',t_df['ticker'][x],': ', company,': added to ',fund, '; ')
                position = '{}{}{}{}'.format('the stock is position ', x+1,' out of ',len(t_df['rank']))
                total = added + position
                with open(summary_file, 'a+') as file:
                    file.write(total)
                    file.write('\n')
            x+=1
        y+=1

def median_mode_change_in_shares(delta_filepath_list):
    """The purpose is to note a fringe indicator"""
    # Indicator: sometimes more than half the stocks in a fund will be sold off by the same %, ie -1.58% shares sold
    # This information may not be critical, but it may paint a unique picture of the ARK fund
    mode_median_message_list = []
    for fund in delta_filepath_list:
        # create a data frame for the today file and the yesterday file
        delta_df = pd.read_csv(fund)

        # in the change % of shares of each company in each fund, find the mode, and how many times the mode appears
        mode_count=0
        mode = delta_df['d_shares_pct'].mode()[0]
        for line in delta_df['d_shares_pct']:
            if line == mode:
                mode_count += 1
        mode_msg = 'MODE: ' + str(mode_count) + 'oo' + str(len(delta_df['d_shares_pct'])) + '(' + \
                   str(mode) + '% shares). '

        quartile_1_count=0
        quartile_1 = delta_df['d_shares_pct'].quantile(q=0.25, interpolation='linear')
        for line in delta_df['d_shares_pct']:
            if line == quartile_1:
                quartile_1_count += 1
        quartile_1_msg = 'Q1: ' + str(quartile_1_count) + 'oo' + str(len(delta_df['d_shares_pct'])) + \
                         '(' + str(quartile_1) + '% shares). '

        # Count the median repetition
        quartile_2_count = 0
        quartile_2 = delta_df['d_shares_pct'].quantile(q=0.5, interpolation='linear')
        for line in delta_df['d_shares_pct']:
            if line == quartile_2:
                quartile_2_count += 1
        quartile_2_msg = 'Q2: ' + str(quartile_2_count) + 'oo' + str(len(delta_df['d_shares_pct'])) + \
                         '(' + str(quartile_2) + '% shares). '

        quartile_3_count=0
        quartile_3 = delta_df['d_shares_pct'].quantile(q=0.75, interpolation='linear')
        for line in delta_df['d_shares_pct']:
            if line == quartile_3:
                quartile_3_count += 1
        quartile_3_msg = 'Q3: ' + str(quartile_3_count) + 'oo' + str(len(delta_df['d_shares_pct'])) + \
                         '(' + str(quartile_3) + '% shares). '

        # Output message for the median and mode trend
        mode_median_output = '\n\t' + mode_msg + quartile_1_msg + quartile_2_msg + quartile_3_msg

        mode_median_message_list.append(mode_median_output)

    return mode_median_message_list

def changed_x_or_more(ticker, fund_sum_today_list, d_fund_market_value_pct_list, summary_file,
                      mode_median_message_list):
    """Highlight the major moves/changes in the ARK etfs"""
    # For a single stock, a change in share % beyond this threshold (+/-) will output a message detailing the change
    share_pct_threshold = 10
    # For a single stock, a change in rank beyond this threshold (+/-) will output a message detailing the change
    rank_change_threshold = 5
    # For a single stock, a change in share price % beyond this threshold (+/-) will output a msg detailing the change
    share_price_pct_change_threshold = 10
    # For a single stock, a change in market value % beyond this threshold (+/-) will output a msg detailing the change
    market_value_pct_change_threshold = 10


    with open(summary_file, 'a+') as file:
        file.write('\n\nWhat were the major changes to the stocks in each fund?\n\tTriggers:\n\t\tChange in shares: '
                   '+/- ' + str(share_pct_threshold) + '%\n\t\tChange in rank: +/- ' + str(rank_change_threshold) +
                   ' positions\n\t\tChange in share price: +/- ' + str(share_price_pct_change_threshold) + \
                   '%\n\t\tChange in market value (MV): +/-' + str(market_value_pct_change_threshold) + '%')

    # Loop through each company in each fund, and output changes that are beyond the thresholds defined above
    z=0
    for fund in ticker:
        # Give a summary of the overall move of each individual fund; ie total market value and % change
        with open(summary_file, 'a+') as file:
            file.write('\n\n' + fund + ': $' + str(f'{fund_sum_today_list[z]:,}') + ' (' + \
                       str(d_fund_market_value_pct_list[z]) + ' %MV):' + mode_median_message_list[z])
        z+=1

        # Get the latest file in the delta folder
        directory = os.path.join(os.getcwd(), fund, 'delta')
        delta_files = [os.path.join(directory, x) for x in os.listdir(directory) if fund in x]
        delta_files = sorted(delta_files, reverse=True)
        # Create a data frame for the latest file in the delta folder
        d_df = pd.read_csv(delta_files[0])

        # Loop through each company in the ARK fund data frame created above, and output messages if the share price,
        # number of shares, or rank is beyond the threshold values defined above
        x=0
        for company in d_df['company']:
            # For a company that is triggered by a threshold metric, this formatting will follow the ticker

            formatting = '\n\n\t' + \
                        str(d_df['ticker'][x]) + ': ' + company + ': ' + \
                        str(d_df['rank'][x]-100) + 'oo' + str(len(d_df['rank'])) + '(' + \
                        str(-d_df['d_rank'][x].astype(int)) + '): $' + \
                        str(d_df['yesterday_share_price'][x]) + ' --> $' + str(d_df['share price'][x]) + '(' + \
                        str(d_df['d_share_price_pct'][x]) + '%): ' + \
                        str(f"{d_df['d_shares'][x].astype(int):,}") + ' shares(' + \
                        str(d_df['d_shares_pct'][x]) + '%): ' + \
                        str(d_df['weight(%)'][x]) + ' wt%(' + \
                        str(d_df['d_weight(%)'][x].round(decimals=2)) + '%)(' + \
                        str(d_df['d_weight_pct_pct'][x]) + '%%): $' + \
                        str(f"{d_df['market value($)'][x].astype(int):,}") + ' MV(' + \
                        str(d_df['d_market_value($)_pct'][x].round(decimals=2)) + ' %MV)'

            # If statements will result in an output to the summary file if a company metric is greater than the
            # threshold metrics limits defined above
            if d_df['d_shares_pct'][x] > share_pct_threshold or \
                    d_df['d_shares_pct'][x] < -share_pct_threshold or \
                    d_df['d_rank'][x] > rank_change_threshold or \
                    d_df['d_rank'][x] < -rank_change_threshold or \
                    d_df['d_share_price_pct'][x] > share_price_pct_change_threshold or \
                    d_df['d_share_price_pct'][x] < -share_price_pct_change_threshold or \
                    d_df['d_market_value($)_pct'][x] > market_value_pct_change_threshold or \
                    d_df['d_market_value($)_pct'][x] < -market_value_pct_change_threshold:
                with open(summary_file, 'a+') as file:
                    file.write(formatting)

            if d_df['d_shares_pct'][x] > share_pct_threshold:
                with open(summary_file, 'a+') as file:
                    file.write('\n\t\t(+) % of shares: ' + str(d_df['d_shares_pct'][x]) + '%')

            if d_df['d_shares_pct'][x] < -share_pct_threshold:
                with open(summary_file, 'a+') as file:
                    file.write('\n\t\t(-) % of shares: ' + str(d_df['d_shares_pct'][x]) + '%')

            if d_df['d_rank'][x] > rank_change_threshold:
                with open(summary_file, 'a+') as file:
                    file.write('\n\t\t(-) rank: ' + str(-d_df['d_rank'][x].astype(int)))

            if d_df['d_rank'][x] < -rank_change_threshold:
                with open(summary_file, 'a+') as file:
                    file.write('\n\t\t(+) rank: ' + str(-d_df['d_rank'][x].astype(int)))

            if d_df['d_share_price_pct'][x] > share_price_pct_change_threshold:
                with open(summary_file, 'a+') as file:
                    file.write('\n\t\t(+) share price %: ' + str(d_df['d_share_price_pct'][x]) + '%')

            if d_df['d_share_price_pct'][x] < -share_price_pct_change_threshold:
                with open(summary_file, 'a+') as file:
                    file.write('\n\t\t(-) share price %: ' + str(d_df['d_share_price_pct'][x]) + '%')

            if d_df['d_market_value($)_pct'][x] > market_value_pct_change_threshold:
                with open(summary_file, 'a+') as file:
                    file.write('\n\t\t(+) % of MV: ' + str(d_df['d_market_value($)_pct'][x]) + '%')

            if d_df['d_market_value($)_pct'][x] < -market_value_pct_change_threshold:
                with open(summary_file, 'a+') as file:
                    file.write('\n\t\t(-) % of MV ' + str(d_df['d_market_value($)_pct'][x]) + '%')

            x+=1

def remove_duplicate_lines():
    """to remove repeat line"""
    lines_seen = set()  # holds lines already seen
    outfile = open(r'C:\python_projects\ark_invest_7\summary\summary_2021_03_99.txt', "w")
    for line in open(r'C:\python_projects\ark_invest_7\summary\summary_2021_03_01.txt', "r"):
        if line not in lines_seen:  # not a duplicate
            outfile.write(line)
            lines_seen.add(line)
    outfile.close()
